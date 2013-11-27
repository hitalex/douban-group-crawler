#coding:utf8

"""
根据已经抓取到的Group id，抓取每个group的topic列表
"""
import sys

from urlparse import urljoin,urlparse
from collections import deque
from threading import Lock
import traceback
import logging
import time
from datetime import datetime
import pdb
import codecs # for file encodings
import os

from bs4 import BeautifulSoup 
from lxml import etree # use XPath from lxml

from webPage import WebPage
from threadPool import ThreadPool
from patterns import *
from models import Group
from database import Database
from logconfig import congifLogger

import stacktracer

log = logging.getLogger('Main.TopicCrawler')


class TopicCrawler(object):

    def __init__(self, group_id, thread_num, group_info_path, topic_list_path, max_topics_num = 1000):
        """
        `group_id`          待抓取的group id
        `thread_num`         抓取的线程
        `group_info_path`   存储group本身的信息文件路径
        `topic_list_path`   保存所有的topic id list的文件路径
        """
        #线程池,指定线程数
        self.thread_pool = ThreadPool(thread_num)
        # 保存topic的线程
        self.save_thread = ThreadPool(1)

        # 写数据库的线程
        #self.DBThread = ThreadPool(1)
                
        # 保存group相关信息
        self.group_info_path = group_info_path
        self.topic_list_path = topic_list_path
        
        # 已经访问的页面: Group id ==> True or False
        self.visited_href = set()
        #待访问的小组讨论页面
        self.unvisited_href = deque()
        # 访问失败的页面链接
        self.failed_href = set()
        
        self.lock = Lock() #线程锁
        
        self.group_id = group_id
        self.group_info = None # models.Group
        
        # 抓取结束有两种可能：1）抓取到的topic数目已经最大；2）已经将所有的topic全部抓取
        # 只保存topic id
        self.topic_list = list()

        self.is_crawling = False
        
        # self.database =  Database("DoubanGroup.db")
        
        # 每个Group抓取的最大topic个数
        self.MAX_TOPICS_NUM = max_topics_num
        #self.MAX_TOPICS_NUM = float('inf')
        # 每一页中显示的最多的topic数量，似乎每页中不一定显示25个topic
        #self.MAX_TOPICS_PER_PAGE = 25

    def start(self):
        print '\nStart Crawling topic list...\n'
        self.is_crawling = True
        self.thread_pool.startThreads()
        self.save_thread.startThreads()
        
        # 打开需要存储的文件
        self.group_info_file = codecs.open(self.group_info_path, 'w', 'utf-8')
        self.topic_list_file = codecs.open(self.topic_list_path, 'w', 'utf-8')
        
        url = "http://www.douban.com/group/" + group_id + "/"
        print "Add start url:", url
        self.unvisited_href.append(url)
        url = "http://www.douban.com/group/" + group_id + "/discussion?start=0"
        print "Add start urls:", url
        self.unvisited_href.append(url)
        
        #分配任务,线程池并发下载当前深度的所有页面（该操作不阻塞）
        self._assignInitTask()
        #等待当前线程池完成所有任务,当池内的所有任务完成时，才进行下一个小组的抓取
        #self.thread_pool.taskJoin()可代替以下操作，可无法Ctrl-C Interupt
        while self.thread_pool.getTaskLeft() > 0:
            #print "Task left: ", self.thread_pool.getTaskLeft()
            time.sleep(3)

        # 存储抓取的结果并等待存储线程结束
        while self.save_thread.getTaskLeft() > 0:
            print 'Wairting for saving thread. Taks left: %d' % self.save_thread.getTaskLeft()
            time.sleep(3)
            
        print "Stroring crawling topic list for: " + group_id
        print "Save to files..."
        #self._saveTopicList()
        
        print "Processing done with group: " + group_id
        log.info("Topic list crawling done with group %s.", group_id)
        
        self.stop()
        assert(self.thread_pool.getTaskLeft() == 0)
        
        # 关闭文件
        self.group_info_file.close()
        self.topic_list_file.close()
        
        print "Main Crawling procedure finished!"

    def stop(self):
        self.is_crawling = False
        self.thread_pool.stopThreads()
        self.save_thread.stopThreads()

    def _assignInitTask(self):
        """取出一个线程，并为这个线程分配任务，即抓取网页
        """ 
        while len(self.unvisited_href) > 0:
            # 从未访问的列表中抽出一个任务，并为其分配thread
            url = self.unvisited_href.popleft()
            self.thread_pool.putTask(self._taskHandler, url)
            # 添加已经访问过的小组id
            self.visited_href.add(url)
            
    def _taskHandler(self, url):
        """ 根据指定的url，抓取网页，并进行相应的访问控制
        """
        print "Visiting : " + url
        webPage = WebPage(url)
        # 抓取页面内容
        flag = webPage.fetch()
        if flag:
            url, pageSource = webPage.getDatas()
            # 抽取小组主页的置顶贴
            match_obj = REGroup.match(url)
            if match_obj is not None:
                group_id = match_obj.group(1)
                # 添加置顶贴的topic列表
                self._addStickTopic(webPage)
                return True
            
            # 抽取普通讨论贴
            match_obj = REDiscussion.match(url)
            if match_obj is not None:
                group_id = match_obj.group(1)
                start = int(match_obj.group(2))
                
                self._addTopicLink(webPage, start)
                return True
                
            log.error("抓取小组讨论列表时，发现网址格式错误。Group ID: %s, URL: %s" % (self.group_id, url))
            
        # if page reading fails
        self.failed_href.add(url)
        return False

    def _addStickTopic(self, webPage):
        """ 访问小组首页，添加置顶贴
        """
        #pdb.set_trace()
        
        group = Group(self.group_id)
        group.parse(webPage)
        
        self.group_info = group
        
        self.save_thread.putTask(self._saveGroupHandler, group)
        
    def _addTopicLink(self, webPage, start):
        '''将页面中所有的topic链接放入对应的topic列表，并同时加入
        下一步要访问的页面
        '''
        #对链接进行过滤:1.只获取http或https网页;2.保证每个链接只访问一次
        #pdb.set_trace()
        url, pageSource = webPage.getDatas()
        hrefs = self._getAllHrefsFromPage(url, pageSource)
        # 找到有效的链接
        topic_list = []
        for href in hrefs:
            # 只有满足小组topic链接格式的链接才会被处理
            match_obj = RETopic.match(href)
            if self._isHttpOrHttpsProtocol(href) and match_obj is not None:
                topic_list.append(match_obj.group(1))
            
        for topic in topic_list: 
            #print "Add group id:", self.group_id, "with topic link: ", href
            self.topic_list.append(topic)
        # 存储已经抓取的topic list
        self.save_thread.putTask(self._saveTopicHandler, topic_list)
                
        # 如果是首页，则需要添加所有的将来访问的页面
        if start == 0:
            print "Adding future visis for Group: " + self.group_id
            self._addFutureVisit(pageSource)
            
    def _saveTopicHandler(self, topic_list):
        """ 将每次从页面中抓取的topic id随时保存到文件中
        """
        for tid in topic_list:
            self.topic_list_file.write(tid + '\n')
        self.topic_list_file.flush()
        os.fsync(self.topic_list_file)
        
    def _saveGroupHandler(self, group):
        """ 保存group的基本信息，比如简介，创建日期等
        `group` models.Group
        """
        #print 'In saving thread'
        # 写入group的基本信息和置顶贴id
        self.group_info_file.write(group.getSimpleString('[=]'))
        self.group_info_file.flush()
        os.fsync(self.group_info_file)

    def _addFutureVisit(self, pageSource):
        """ 访问讨论列表的首页，并添加所有的将来要访问的链接
        """
        #pdb.set_trace()
        if not isinstance(pageSource, unicode):
            # 默认页面采用UTF-8编码
            page = etree.HTML(pageSource.decode('utf-8'))
        else:
            page = etree.HTML(pageSource)
            
        # 目前的做法基于以下观察：在每个列表页面，paginator部分总会显示总的页数
        # 得到总的页数后，便可以知道将来所有需要访问的页面
        paginator = page.xpath(u"//div[@class='paginator']/a")
        last_page = int(paginator[-1].text.strip())
        for i in range(1, last_page):
            # 控制加入topic列表的数量
            if i * 25 >= self.MAX_TOPICS_NUM:
                break
            url = "http://www.douban.com/group/" + self.group_id + "/discussion?start=" + str(i * 25)
            # 向线程池中添加任务：一次性添加
            self.thread_pool.putTask(self._taskHandler, url)
            # 添加已经访问过的小组id
            self.visited_href.add(url)

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
            
    def _saveTopicList(self):
        """将抽取的结果存储在文件中
        Note: 这次是将存储过程放在主线程，将会阻塞抓取过程
        """
        group_id = self.group_id
        this_group = self.group_info
        print "For group %s: number of Stick post: %d, number of regurlar post: %d, total topics is: %d." % \
            (group_id, len(this_group.stick_topic_list), len(self.topic_list), len(this_group.stick_topic_list)+len(self.topic_list))
            
        # 将访问失败的网页存储起来
        log.info('抓取失败的网页：')
        for href in self.failed_href:
            log.info(href)
        
        # 保存Group的本身的信息
        f = open(group_info_path, "w")
        f.write(this_group.__repr__())
        f.close()
        
        # 存储Topic相关信息
        f = open(topic_list_path, 'w')
        for tid in this_group.stick_topic_list:
            f.write(tid + "\n")
            
        f.write("\n")
        for tid in self.topic_list:
            f.write(tid + "\n")
            
        f.close()
        
        self.topic_list = list()
        self.failed_href = set()
        
if __name__ == "__main__":
    stacktracer.trace_start("trace.html",interval=5,auto=True) # Set auto flag to always update file!
    congifLogger("log/topicCrawler.log", 5)
    
    group_id_list = []
    if len(sys.argv) <= 1:
        print "Group IDs were not provided."
        sys.exit()
    # add group ids
    for i in range(1, len(sys.argv)):
        group_id_list.append(sys.argv[i])
        
    print "Crawling topic list for groups: ", group_id_list
    #tcrawler = TopicCrawler(['FLL', '294806', 'MML'], 5)
    #tcrawler = TopicCrawler(['70612'], 5) # 我们都是学术女小组
    #tcrawler = TopicCrawler(['ustv'], 5) # 美剧fans 小组
    for group_id in group_id_list:
        base_path = '/home/kqc/dataset/douban-group/'
        time_now = datetime.now()
        # 在group-info中只包含group信息和置顶贴的id，TopicList中包含普通topic list
        # 这么做的原因是：并不能保证两者写入的时间先后顺序
        group_info_path = base_path + 'Info-' + group_id + '-' +str(time_now) + '.txt'
        topic_list_path = base_path + 'TopicList-' + group_id + '-' + str(time_now) + '.txt'
        tcrawler = TopicCrawler(group_id, 5, group_info_path, topic_list_path, 10000)
        tcrawler.start()
    
    stacktracer.trace_stop()

