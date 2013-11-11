#coding:utf8

"""
根据已经抓取到的每个小组的topic列表，针对具体的每个topic抓取评论
主题框架于topic_crawler.py相似
"""

from urlparse import urljoin,urlparse
from collections import deque
import traceback
import logging
import time
from datetime import datetime
import pdb
import codecs # for file encodings
import os
import sys

from bs4 import BeautifulSoup 

from webPage import WebPage
from threadPool import ThreadPool
from patterns import *
from models import Topic
from database import Database
from logconfig import congifLogger

import stacktracer

log = logging.getLogger('Main.CommentCrawler')


class CommentCrawler(object):
    
    def __init__(self, group_id, topic_id_list, thread_num, topic_info_path, comment_info_path):
        """
        `group_id` 当前的Group id
        `topic_id_list` 需要抓取的topic id的list
        `thread_num` 开启的线程数目
        `topic_info_path` 存储topic信息的文件
        `comment_info_path` 存储comment信息的文件
        """
        
        #线程池,指定线程数
        self.thread_pool = ThreadPool(thread_num)

        # 保证同时只有一个线程在写文件
        self.save_thread = ThreadPool(1)
        
        self.topic_info_path = topic_info_path
        self.comment_info_path = comment_info_path
        self.base_path = 'data/'
        
        # 已经访问的页面: Group id ==> True or False
        self.visited_href = set()
        # 抓取失败的topic id
        self.failed = set()
        
        
        # 依次为每个小组抽取topic评论
        self.group_id = group_id
        self.topic_id_list = topic_id_list # 等待抓取的topic列表
        
        # 存储结果
        # topic ID ==> Topic对象
        self.topic_dict = dict()
        # 存放下一个处理的评论页数： topic ID ==> 1,2,3...
        self.next_page = dict()
        # 已经抓取完毕的topic id集合
        self.finished = set()

        self.is_crawling = False
        
        # 每个topic抓取的最多comments个数
        #self.MAX_COMMETS_NUM = 5000
        self.MAX_COMMETS_NUM = float('inf')
        
        # 每页的评论数量
        self.COMMENTS_PER_PAGE = 100

    def start(self):
        print '\nStart Crawling comment list for group: ' + self.group_id + '...\n'
        self.is_crawling = True
        self.thread_pool.startThreads()
        self.save_thread.startThreads()
        
        # 打开需要存储的文件
        self.topic_info_file = codecs.open(self.topic_info_path, 'w', 'utf-8')
        self.comment_info_file = codecs.open(self.comment_info_path, 'w', 'utf-8')
        
        self.topic_id_list = list(set(self.topic_id_list)) # 消除重复的topic id
        print "Total topics in group %s: %d." % (self.group_id, len(self.topic_id_list))
        
        # 初始化添加任务
        for topic_id in self.topic_id_list:
            url = "http://www.douban.com/group/topic/" + topic_id + "/"
            self.thread_pool.putTask(self._taskHandler, url)
            # 下一页评论类似：http://www.douban.com/group/topic/35082953/?start=100
            self.next_page[topic_id] = 1
        
        # 完全抛弃之前的抽取深度的概念，改为随时向thread pool推送任务
        while True:
            # 保证任何时候thread pool中的任务数为线程数的2倍
            print "Check threalPool queue..."
            while self.thread_pool.getTaskLeft() < self.thread_pool.threadNum * 2:
                # 获取未来需要访问的链接
                url = self._getFutureVisit()
                if url is not None: 
                    self.thread_pool.putTask(self._taskHandler, url)
                else: # 已经不存在下一个链接
                    break
            # 每隔一秒检查thread pool的队列
            time.sleep(2)
            # 检查是否处理完毕
            if len(self.finished) == len(self.topic_id_list):
                break
            elif len(self.finished) > len(self.topic_id_list):
                assert(False)
            print 'Total topics: %d, Finished topic: %d' % (len(self.topic_id_list), len(self.finished))
            
            remain = set(self.topic_id_list) - self.finished
            if len(remain) < 5:
                print 'Unfinished: ', remain
                
        # 等待线程池中所有的任务都完成
        print "Totally visited: ", len(self.visited_href)
        #pdb.set_trace()
        while self.thread_pool.getTaskLeft() > 0:
            print "Task left in threadPool: ", self.thread_pool.getTaskLeft()
            print "Task queue size: ", self.thread_pool.taskQueue.qsize()
            print "Running tasks: ", self.thread_pool.running
            time.sleep(2)
        
        # 检查保存线程完成情况
        while self.save_thread.getTaskLeft() > 0:
            print "Task left in save thread: ", self.save_thread.getTaskLeft()
            print "Task queue size: ", self.save_thread.taskQueue.qsize()
            print "Running tasks: ", self.save_thread.running
            time.sleep(2)
        
        # 记录抓取失败的topic id
        log.info('抓取失败的topic id：')
        s = ''
        for topic_id in self.failed:
            s += (topic_id + '\n')
        log.info('\n' + s)
        
        print "Terminating all threads..."
        self.stop()
        assert(self.thread_pool.getTaskLeft() == 0)
        
        self.topic_info_file.close()
        self.comment_info_file.close()
        
        print "Main Crawling procedure finished!"
        
        print "Start to save result..."
        #self._saveCommentList()
        #self._saveComment2file()
        log.info("Processing done with group: %s" % (self.group_id))

    def stop(self):
        self.is_crawling = False
        self.thread_pool.stopThreads()
        self.save_thread.stopThreads()
        
    def _taskHandler(self, url):
        """ 根据指定的url，抓取网页，并进行相应的访问控制
        """      
        print "Visiting : " + url
        webPage = WebPage(url)
        
        # 抓取页面内容
        flag = webPage.fetch()
        match_obj = RETopic.match(url)
        match_obj2 = REComment.match(url)
        
        if flag:
            if match_obj is not None:
                topic_id = match_obj.group(1)
                topic = Topic(topic_id, self.group_id)
                comment_list = topic.parse(webPage, isFirstPage = True) # First page parsing
                self.topic_dict[topic_id] = topic
                # 保存到文件
                self.save_thread.putTask(self._saveHandler, comment_list, topic = topic)
            elif match_obj2 is not None:
                topic_id = match_obj2.group(1)
                start = int(match_obj2.group(2))
                # 抽取非第一页的评论数据
                if topic_id in self.topic_dict:
                    topic = self.topic_dict[topic_id]
                    if topic is None:
                        log.error('未知程序错误：结束topic id为%s的抽取，释放内存。' % topic_id)
                        self.topic_dict[topic_id] = None
                        return False
                else:
                    # 这里的含义为：必须先处理第一页的评论，否则该topic_id不会作为self.topic_dict的键出现
                    log.error('错误：必须先抽取第一页的评论数据：topic id: %s' % topic_id)
                    self.failed.add(topic_id)
                    self.finished.add(topic_id)
                    return False
                    
                comment_list = topic.parse(webPage, isFirstPage = False) # non-firstpage parsing
                # 保存到文件
                self.save_thread.putTask(self._saveHandler, comment_list, topic = None)
            else:
                #pdb.set_trace()
                log.info('Topic链接格式错误：%s in Group: %s.' % (url, self.group_id))
            # 判断抓取是否结束，如果结束，则释放dict内存
            # 这个很重要，因为随着topic数量增多，内存会占很多
            if topic.isComplete():
                # 对评论进行排序，并查找quote comment
                self.topic_dict[topic_id].sortComment()
                self.save_thread.putTask(self._saveTopicHandler, self.topic_dict, topic_id)
                #self.topic_dict[topic_id] = None        # 释放资源
                self.finished.add(topic_id)
                log.info('Topic: %s 抓取结束。' % topic_id)
                
            self.visited_href.add(url)
            return True
        else:
            # 处理抓取失败的网页集合
            # 只要一个网页抓取失败，则加入到finished
            if match_obj is not None:
                # 讨论贴的第一页就没有抓到，则将其列入finished名单中
                topic_id = match_obj.group(1)
            elif match_obj2 is not None:
                topic_id = match_obj2.group(1)
                start = int(match_obj2.group(2))
            else:
                log.info('Topic链接格式错误：%s in Group: %s.' % (url, self.group_id))
            
            # 添加抓取失败的topic id和标记抓取结束的topic
            self.failed.add(topic_id)
            self.finished.add(topic_id) # 有可能已经记录了一些某些topic的信息
            self.visited_href.add(url)
            return False

    def _saveHandler(self, comment_list, topic):
        """ 将topic信息和comemnt信息保存到文件中
        注意：这里将topic信息和comment信息分开存储
        注意：这里每一行存储一个topic或者comment，不再使用ROWEND符号作为结束
        Note: 使用此方式保存comment quote会产生错误。
        """
        # 先保存comment_list id
        # 判断是否是第一次保存该topic
        if topic != None: # 如果是第一次保存，则需要保存topic的基本信息
            s = topic.getSimpleString('[=]')
            #self.topic_info_file.write(s + '\n[*ROWEND*]\n')
            self.topic_info_file.write(s + '\n')
        # 保存comment信息
        for comment in comment_list:
            s = comment.getSimpleString('[=]')
            #self.comment_info_file.write(s + '\n[*ROWEND*]\n')
            self.comment_info_file.write(s + '\n')
            
        # 保证已经写入到磁盘上，这样可以随时终止
        self.topic_info_file.flush()
        os.fsync(self.topic_info_file) # The method fsync() forces write of file with file descriptor fd to disk
        
        self.comment_info_file.flush()
        os.fsync(self.comment_info_file)
        
    def _saveTopicHandler(self, topic_dict, topic_id):
        """ 存储抓取完毕的帖子信息以及其对应的Comment。
        不过，跟_saveHandler函数不同的是，这里是按照topic id存储
        @topic_dict 存储topic信息的字典
        @topic_id 需要存储的topic id
        """
        topic = topic_dict[topic_id]
        topic_path = self.base_path + group_id + '/' + topic_id + '-info.txt'
        # 存储topic本身的信息
        f = codecs.open(topic_path, 'w', 'utf-8')
        s = topic.getSimpleString('[=]')
        f.write(s + '\n')
        #f.write('[*ROWEND*]')
        
        # 存储comment信息,存储到相同的文件中
        for comment in topic.comment_list:
            s = comment.getSimpleString('[=]')
            #f.write(s + '\n[*ROWEND*]\n')
            f.write(s + '\n')
        f.close()
        
        self.topic_dict[topic_id] = None        # 释放资源
        log.info("Topic: %s 存储结束" % topic_id)

    def _getFutureVisit(self):
        """根据当前的访问情况，获取下一个要访问的网页
        """
        for topic_id in self.topic_dict:
            if topic_id in self.finished:
                continue
            topic = self.topic_dict[topic_id]
            if topic is None:
                continue
            if topic.max_comment_page <= 0:
                # 还未处理该topic的首页
                continue
            elif topic.max_comment_page == 1:
                # 该topic只有首页有评论
                continue
            else:
                # 该topic有多页评论
                next_start = self.next_page[topic_id]
                url = "http://www.douban.com/group/topic/" + topic_id + "/?start=" + str(next_start * self.COMMENTS_PER_PAGE)
                if next_start <= topic.max_comment_page-1:
                    self.next_page[topic_id] = next_start + 1
                    return url
                else:
                    continue
        
        return None        

    def _getAllHrefsFromPage(self, url, pageSource):
        '''解析html源码，获取页面所有链接。返回链接列表'''
        hrefs = []
        soup = BeautifulSoup(pageSource)
        results = soup.find_all('a',href=True)
        for a in results:
            #必须将链接encode为utf8, 因为中文文件链接如 http://aa.com/文件.pdf 
            #在bs4中不会被自动url编码，从而导致encodeException
            href = a.get('href').encode('utf8')
            if not href.startswith('http'):
                href = urljoin(url, href)#处理相对链接的问题
            hrefs.append(href)
        return hrefs

    def _isHttpOrHttpsProtocol(self, href):
        protocal = urlparse(href).scheme
        if protocal == 'http' or protocal == 'https':
            return True
        return False
        
    def _getAlreadyVisitedNum(self):
        #visitedGroups保存已经分配给taskQueue的链接，有可能链接还在处理中。
        #因此真实的已访问链接数为visitedGroups数减去待访问的链接数
        if len(self.visited_href) == 0:
            return 0
        else:
            return len(self.visited_href) - self.thread_pool.getTaskLeft()
    
    '''
    def _saveCommentList(self):
        """将抽取的结果存储在文件中，包括存储topic内容和评论内容
        Note: 这次是将存储过程放在主线程，将会阻塞抓取过程
        NOTE: 此函数已经不再使用
        """
        # 如果不存在目录，则创建它
        path = "data/" + self.group_id + "/"
        if not os.path.exists(path):
            os.mkdir(path)
            
        for topic_id in self.topic_id_list:
            topic = self.topic_dict[topic_id]
            path = "data/" + self.group_id + "/" + topic_id + ".txt"
            f = codecs.open(path, "w", "utf-8", errors='replace')
            f.write(topic.__repr__())
            f.close()
            
        # save the failed hrefs
        f = open("data/"+self.group_id+"/failed.txt", "w")
        for href in self.failed:
            f.write(href + "\n")
        f.close()
        
        # write comment structures
        path = "structure/" + self.group_id + "/"
        if not os.path.exists(path):
            os.mkdir(path)
        for topic_id in self.topic_dict:
            atopic = self.topic_dict[topic_id]
            path = "structure/" + self.group_id + "/" + topic_id + ".txt"
            f = codecs.open(path, "w", "utf-8", errors='replace')
            # 每一行：评论id，评论用户id，（引用评论id，引用评论的用户id）
            for comment in atopic.comment_list:
                f.write(comment.cid + " " + comment.user_id)
                if comment.quote is not None:
                    f.write(" " + comment.quote.cid + " " + comment.quote.user_id)
                f.write("\n")
            f.close()
            
    def _saveComment2file(self):
        """ 直接将抓取结果存入文件中
        """
        ftopic = open(self.topic_info_path, 'w')
        fcomment = open(self.comment_info_path , 'w')
        for topic_id in self.topic_dict:
            topic = self.topic_dict[topic_id]
            s = topic.getSimpleString(delimiter = '[=]')
            ftopic.write(s + '\n[*ROWEND*]\n')
            for comment in topic.comment_list:
                cs = comment.getSimpleString(delimiter = '[=]')
                fcomment.write(cs + '\n[*ROWEND*]\n')
                
        ftopic.close()
        fcomment.close()
     '''
        
if __name__ == "__main__":
    LINE_FEED = "\n" # 采用windows的换行格式
    stacktracer.trace_start("trace.html",interval=5,auto=True) # Set auto flag to always update file!
    congifLogger("log/comment_crawler.log", 5)
    #group_id_list = ['FLL', '294806', 'MML']
    #group_id_list = ['test']
    #group_id_list = ['70612', 'FLL']
    group_id_list = []
    if len(sys.argv) <= 1:
        print "Group IDs were not provided."
        sys.exit()
    # add group ids
    for i in range(1, len(sys.argv)):
        group_id_list.append(sys.argv[i])
        
    print "Crawling comments for groups: ", group_id_list
    
    MAX_TOPIC_NUM = float('inf') # 每个小组最多处理的topic的个数
    group_id = group_id_list[0]
    """
    for index in range(8,9):
        # 读取topic列表
        f = open('tables/ustv/TopicList-ustv-remain-' + str(index), 'r')
        #f = open('tables/ustv/topic-list-test.txt', 'r')
        topic_list = []
        for line in f:
            line = line.strip()
            if line is not "":
                topic_list.append(line)
                #if len(topic_list) >= MAX_TOPIC_NUM:
                #    break
        f.close()
        
        time_now = datetime.now()
        topic_path = 'tables/' + group_id + '/TopicInfo-' + group_id + '-' + str(time_now) + '-raw-' + str(index)
        comment_path = 'tables/' + group_id + '/CommentInfo-' + group_id + '-' + str(time_now) + '-raw-' + str(index)
        comment_crawler = CommentCrawler(group_id, topic_list, 5, topic_path, comment_path)
        comment_crawler.start()
    """
    # 抓取insidestory
    f = open('data/' + group_id + '/TopicList-' + group_id + '.txt', 'r')
    topic_list = []
    for line in f:
        line = line.strip()
        if line is not "":
            topic_list.append(line)
    f.close()
    
    #time_now = datetime.now()
    topic_path = 'data/' + group_id + '/TopicInfo-' + group_id + '-raw-all.txt'
    comment_path = 'data/' + group_id + '/CommentInfo-' + group_id + '-raw-all.txt'
    comment_crawler = CommentCrawler(group_id, topic_list, 5, topic_path, comment_path)
    comment_crawler.start()
    
    print "Done"
    stacktracer.trace_stop()
