#coding:utf8

"""
group_crawler.py
~~~~~~~~~~~~~
根据起始的一些URL，抓取页面中的group id，并需要指定抓取深度，最终存储在数据库中。
抓取的内容为：小组的介绍、置顶帖子。
"""

from urlparse import urljoin,urlparse
from collections import deque
from threading import Lock
from locale import getdefaultlocale
import logging
import time
import pdb
import codecs # for file encodings

from bs4 import BeautifulSoup 

from webPage import WebPage
from threadPool import ThreadPool
from models import Group
from patterns import *
from database import Database
import traceback

log = logging.getLogger('Main.crawler')


class GroupCrawler(object):

    def __init__(self, args, startURLs):
        #指定网页深度
        self.depth = args.depth  
        #标注初始爬虫深度，从1开始
        self.currentDepth = 1  
        #指定关键词,使用console的默认编码来解码
        #self.keyword = args.keyword.decode(getdefaultlocale()[1]) 
        #数据库
        self.database =  Database(args.dbFile)
        # store group ids to fils, using UTF-8
        self.groupfile = codecs.open("GroupID.txt", "w", "UTF-8")
        #线程池,指定线程数
        self.threadPool = ThreadPool(args.threadNum)  
        #已访问的小组id
        self.visitedGroups = set()
        #待访问的小组id
        self.unvisitedGroups = deque()
        
        # 所有的Group信息
        self.groupInfo = []
        
        self.lock = Lock() #线程锁

        #标记爬虫是否开始执行任务
        self.isCrawling = False
        # 添加尚未访问的小组首页
        for url in startURLs:
            match_obj = REGroup.match(url)
            print "Add start urls:", url
            assert(match_obj != None)
            self.unvisitedGroups.append(match_obj.group(1))
        
        # 一分钟内允许的最大访问次数
        self.MAX_VISITS_PER_MINUTE = 10
        # 当前周期内已经访问的网页数量
        self.currentPeriodVisits = 0
        # 将一分钟当作一个访问周期，记录当前周期的开始时间
        self.periodStart = time.time() # 使用当前时间初始化

    def start(self):
        print '\nStart Crawling\n'
        if not self._isDatabaseAvaliable():
            print 'Error: Unable to open database file.\n'
        else:
            self.isCrawling = True
            self.threadPool.startThreads() 
            self.periodStart = time.time() # 当前周期开始
            # 按照depth来抓取网页
            while self.currentDepth < self.depth+1:
                #分配任务,线程池并发下载当前深度的所有页面（该操作不阻塞）
                self._assignCurrentDepthTasks ()
                #等待当前线程池完成所有任务,当池内的所有任务完成时，即代表爬完了一个网页深度
                #self.threadPool.taskJoin()可代替以下操作，可无法Ctrl-C Interupt
                while self.threadPool.getTaskLeft() > 0:
                    print "Task left: ", self.threadPool.getTaskLeft()
                    time.sleep(3)
                print 'Depth %d Finish. Totally visited %d links. \n' % (
                    self.currentDepth, len(self.visitedGroups))
                log.info('Depth %d Finish. Total visited Links: %d\n' % (
                    self.currentDepth, len(self.visitedGroups)))
                self.currentDepth += 1
            self.stop()
            assert(self.threadPool.getTaskLeft() == 0)
            print "Main Crawling procedure finished!"

    def stop(self):
        self.isCrawling = False
        self.threadPool.stopThreads()
        # save group ids to file
        for group_id in self.visitedGroups:
            self.groupfile.write(group_id + "\n")
        self.groupfile.close()
        self.database.close()

    def getAlreadyVisitedNum(self):
        #visitedGroups保存已经分配给taskQueue的链接，有可能链接还在处理中。
        #因此真实的已访问链接数为visitedGroups数减去待访问的链接数
        if len(self.visitedGroups) == 0:
            return 0
        else:
            return len(self.visitedGroups) - self.threadPool.getTaskLeft()

    def _assignCurrentDepthTasks(self):
        """取出一个线程，并为这个线程分配任务，即抓取网页，并进行相应的访问控制
        """
        # 判断当前周期内访问的网页数目是否大于最大数目
        if self.currentPeriodVisits > self.MAX_VISITS_PER_MINUTE - 1:
            # 等待所有的网页处理完毕
            while self.threadPool.getTaskLeft() > 0:
                print "Waiting period ends..."
                time.sleep(1)
            timeNow = time.time()
            seconds = timeNow - self.periodStart
            if  seconds < 60: # 如果当前还没有过一分钟,则sleep
                time.sleep(int(seconds + 3))
            self.periodStart = time.time() # 重新设置开始时间
            self.currentPeriodVisits = 0
        # 从未访问的列表中抽出，并为其分配thread
        while len(self.unvisitedGroups) > 0:
            group_id = self.unvisitedGroups.popleft()
            #向任务队列分配任务
            url = "http://www.douban.com/group/" + group_id + "/"
            self.threadPool.putTask(self._taskHandler, url)
            # 添加已经访问过的小组id
            self.visitedGroups.add(group_id)
            
    def _taskHandler(self, url):
        """ 根据指定的url，抓取网页
        """
        print "Visiting : " + url
        webPage = WebPage(url)
        # 抓取页面内容
        flag = webPage.fetch()
        if flag:
            self.lock.acquire() #锁住该变量,保证操作的原子性
            self.currentPeriodVisits += 1
            self.lock.release()
            
            self._saveTaskResults(webPage)
            self._addUnvisitedGroups(webPage)
            return True
            
        # if page reading fails
        return False

    def _saveTaskResults(self, webPage):
        """将小组信息写入数据库
        """
        url, pageSource = webPage.getDatas()
        # 产生一个group对象
        dbgroup = Group(url, pageSource)
        # 写入数据库
        self.database.saveGroupInfo(dbgroup)
        
    def _addUnvisitedGroups(self, webPage):
        '''添加未访问的链接，并过滤掉非小组主页的链接。将有效的url放进UnvisitedGroups列表'''
        #对链接进行过滤:1.只获取http或https网页;2.保证每个链接只访问一次
        url, pageSource = webPage.getDatas()
        hrefs = self._getAllHrefsFromPage(url, pageSource)
        for href in hrefs:
            #print "URLs in page: ", href
            match_obj = REGroup.match(href)
            # 只有满足小组主页链接格式的链接才会被处理
            if self._isHttpOrHttpsProtocol(href) and (match_obj is not None):
                #pdb.set_trace()
                group_id = match_obj.group(1)
                #print "Group link: " + href
                if not self._isGroupRepeated(group_id):
                    # 将小组id放入待访问的小组列表中去
                    print "Add group id:", group_id
                    self.unvisitedGroups.append(group_id)

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

    def _isGroupRepeated(self, group_id):
        if (group_id in self.visitedGroups) or (group_id in self.unvisitedGroups):
            return True
        return False

    def _isDatabaseAvaliable(self):
        if self.database.isConn():
            return True
        return False

    def selfTesting(self, args):
        url = 'http://www.douban.com/group/insidestory/'
        print '\nVisiting http://www.douban.com/group/insidestory/'
        #测试网络,能否顺利获取百度源码
        pageSource = WebPage(url).fetch()
        if pageSource == None:
            print 'Please check your network and make sure it\'s connected.\n'
        #数据库测试
        elif not self._isDatabaseAvaliable():
            print 'Please make sure you have the permission to save data: %s\n' % args.dbFile
        #保存数据
        else:
            #self._saveTaskResults(url, pageSource)
            print 'Create logfile and database Successfully.'
            print 'Already save Baidu.com, Please check the database record.'
            print 'Seems No Problem!\n'
