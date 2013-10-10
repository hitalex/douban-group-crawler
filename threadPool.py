#coding:utf8

"""
threadPool.py
~~~~~~~~~~~~~

该模块包含工作线程与线程池的实现。
"""

import traceback
from threading import Thread, Lock
from Queue import Queue,Empty
import logging
import time
import pdb

log = logging.getLogger('Main.threadPool')


class Worker(Thread):

    def __init__(self, threadPool, ID):
        Thread.__init__(self)
        self.threadPool = threadPool
        self.daemon = True
        self.state = None
        self.ID = ID    # 标识thread的index
        self.start()

    def stop(self):
        self.state = 'STOP'

    def run(self):
        while 1:
            """
            if self.ID == 1:
                print "Thread ID: ", self.ID, " loops with state = ", self.state
            """
            if self.state == 'STOP':
                break
            # 将整个getTask函数设置为原子块
            #if self.ID == 1:
            #    pdb.set_trace()
            self.threadPool.taskLock.acquire()
            func, args, kargs = self.threadPool.getTask()
            self.threadPool.taskLock.release()
            if func is None or args is None or kargs is None:
                continue
            
            try:
                #print "Thread ID: ", str(self.ID), " with thread state: ", self.state
                self.threadPool.increaseRunsNum() 
                # 抓取网页
                func(*args, **kargs) 
                # TODO : 搞清楚如何利用 resultQueue
                """
                if result:
                    #the func, i.e. _taskHandler always returns none, so putTaskResult will never be called
                    self.threadPool.putTaskResult(*result)
                """
                self.threadPool.taskDone() # 通知Queue一个任务已经执行完毕
            except Exception, e:
                log.critical(traceback.format_exc())
            finally:
                self.threadPool.decreaseRunsNum()


class ThreadPool(object):

    def __init__(self, threadNum, max_tasks_per_period = 10, seconds_per_period = 30):
        self.pool = [] #线程池
        self.threadNum = threadNum  #线程数
        self.runningLock = Lock() #线程锁
        self.taskLock = Lock() # getTask函数的锁
        self.running = 0    #正在run的线程数
        self.taskQueue = Queue() #任务队列
        self.resultQueue = Queue() #结果队列, but never used here
        
        # 一分钟内允许的最大访问次数
        self.max_tasks_per_period = max_tasks_per_period
        # 定制每分钟含有的秒数
        self.seconds_per_period = seconds_per_period 
        # 当前周期内已经访问的网页数量
        self.currentPeriodVisits = 0
        # 将一分钟当作一个访问周期，记录当前周期的开始时间
        self.periodStart = time.time() # 使用当前时间初始化
    
    def startThreads(self):
        """Create a certain number of threads and started to run 
        All Workers share the same ThreadPool
        """
        # 开始当前的抓取周期
        self.periodStart = time.time()
        for i in range(self.threadNum): 
            self.pool.append(Worker(self, i))
    
    def stopThreads(self):
        for thread in self.pool:
            thread.stop()
            thread.join()
        del self.pool[:]
    
    def putTask(self, func, *args, **kargs):
        self.taskQueue.put((func, args, kargs))

    def getTask(self, *args, **kargs):
        # 进行访问控制: 判断当前周期内访问的网页数目是否大于最大数目
        if self.currentPeriodVisits >= self.max_tasks_per_period - 2:
            timeNow = time.time()
            seconds = timeNow - self.periodStart
            if  seconds < self.seconds_per_period: # 如果当前还没有过一分钟,则sleep
                remain = self.seconds_per_period - seconds
                print "ThreadPool Waiting for " + str(remain) + " seconds."
                time.sleep(int(remain + 1))

            self.periodStart = time.time() # 重新设置开始时间
            self.currentPeriodVisits = 0
            
        try:
            #task = self.taskQueue.get(*args, **kargs)
            task = self.taskQueue.get_nowait()
        except Empty:
            return (None, None, None)
            
        self.currentPeriodVisits += 1
        
        return task

    def taskJoin(self, *args, **kargs):
        """Queue.join: Blocks until all items in the queue have been gotten and processed.
        """
        self.taskQueue.join()

    def taskDone(self, *args, **kargs):
        self.taskQueue.task_done()

    def putTaskResult(self, *args):
        self.resultQueue.put(args)

    def getTaskResult(self, *args, **kargs):
        return self.resultQueue.get(*args, **kargs)

    def increaseRunsNum(self):
        self.runningLock.acquire()
        self.running += 1 #正在运行的线程数加1
        self.runningLock.release()

    def decreaseRunsNum(self):
        self.runningLock.acquire()
        self.running -= 1 
        self.runningLock.release()

    def getTaskLeft(self):
        #线程池的所有任务包括：
        #taskQueue中未被下载的任务, resultQueue中完成了但是还没被取出的任务, 正在运行的任务
        #因此任务总数为三者之和
        return self.taskQueue.qsize()+self.resultQueue.qsize()+self.running
