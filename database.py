#coding:utf8

"""
database.py
~~~~~~~~~~~~~

该模块提供爬虫所需的sqlite数据库的创建、连接、断开，以及数据的存储功能。
"""

import sqlite3

class Database(object):
    def __init__(self, dbFile):
        try:
            self.conn = sqlite3.connect(dbFile, isolation_level=None, check_same_thread = False) #让它自动commit，效率也有所提升. 多线程共用
        except Exception, e:
            self.conn = None
            raise sqlite3.OperationalError,'Cannot connect database.'
        
        cur = self.conn.cursor()
        cur.execute('''CREATE TABLE IF NOT EXISTS
                        Webpage (id INTEGER PRIMARY KEY AUTOINCREMENT, 
                        url TEXT, 
                        pageSource TEXT,
                        keyword TEXT)''')
        cur.execute('''CREATE TABLE IF NOT EXISTS
                        GroupInfo (id INTEGER PRIMARY KEY AUTOINCREMENT,
                        group_id TEXT,
                        user_id TEXT, 
                        pubdate TEXT,
                        description TEXT,
                        topic_list TEXT)''')
        # topic信息 数据表
        cur.execute('''CREATE TABLE IF NOT EXISTS
                        TopicInfo (id INTEGER PRIMARY KEY AUTOINCREMENT, 
                        topic_id TEXT,
                        group_id TEXT,
                        user_id TEXT, 
                        pubdate TEXT,
                        title TEXT,
                        content TEXT,
                        comment_list TEXT)''')
        # 评论信息 数据表
        cur.execute('''CREATE TABLE IF NOT EXISTS
                        CommentInfo (id INTEGER PRIMARY KEY AUTOINCREMENT, 
                        comment_id TEXT,
                        group_id TEXT,
                        topic_id TEXT,
                        user_id TEXT, 
                        pubdate TEXT,
                        ref_comment_id TEXT,
                        content TEXT)''')
        cur.execute('''CREATE TABLE IF NOT EXISTS
                        'GroupTest' (id INTEGER PRIMARY KEY AUTOINCREMENT,
                        group_id TEXT,
                        user_id TEXT, 
                        pubdate TEXT,
                        description TEXT,
                        topic_list TEXT)''')

    def isConn(self):
        if self.conn:
            return True
        else:
            return False

    def saveData(self, url, pageSource, keyword=''):
        """ 保存网页信息，已弃用
        """
        if self.conn:
            sql='''INSERT INTO Webpage (url, pageSource, keyword) VALUES (?, ?, ?);'''
            self.conn.execute(sql, (url, pageSource, keyword) )
        else :
            raise sqlite3.OperationalError,'Database is not connected. Can not save Data!'
            
    def saveGrouInfo(self, dbgroup, topic_list):
        """ 保存小组信息
        """
        if self.conn:
            s = ""
            # 添加置顶贴
            for tid in dbgroup.stick_topic_list:
                s += (tid + ",")
            # 添加普通讨论贴
            for tid in topic_list:
                s += (tid + ",")
            cur = self.conn.cursor()
            sql='''INSERT INTO GroupInfo (group_id, user_id, pubdate, description, topic_list) VALUES (?, ?, ?, ?, ?);'''
            cur.execute(sql, (dbgroup.group_id, dbgroup.user_id, str(dbgroup.pubdate), dbgroup.desc, s) )
        else :
            raise sqlite3.OperationalError,'Database is not connected. Can not save Data!'
            
    def saveTopicInfo(self, topic):
        """保存Topic相关信息
        Note: 每次在保存topic信息的时候，保存comment信息
        """
        if self.conn:
            s = u""
            # 添加评论id列表
            for comment in topic.comment_list:
                s += (comment.cid + ",")
            cur = self.conn.cursor()
            # 只记录comment id列表
            sql='''INSERT INTO TopicInfo (topic_id, group_id, user_id, pubdate, title, content, comment_list) VALUES (?, ?, ?, ?, ?, ?, ?);'''
            cur.execute(sql, (topic.topic_id, topic.group_id, topic.user_id, str(topic.pubdate), topic.tilte, topic.content, s) )
            # 保存评论的内容信息
            self.saveCommentInfo(topic)
        else:
            raise sqlite3.OperationalError,'Database is not connected. Can not save Data!'
            
    def saveCommentInfo(self, topic):
        """保存Comment相关信息
        """
        if self.conn:
            s = ""
            records = []
            # 添加评论id列表
            for comment in topic.comment_list:
                if comment.quote is None:
                    ref_comment_id = ""
                else:
                    ref_comment_id = comment.quote.cid
                r = (comment.cid, comment.group_id, comment.topic_id, comment.user_id, comment.pubdate, ref_comment_id, comment.content)
                records.append(r)
                
            cur = self.conn.cursor()
            sql='''INSERT INTO CommentInfo (comment_id, group_id, topic_id, user_id, pubdate, ref_comment_id, content) VALUES (?, ?, ?, ?, ?, ?, ?);'''
            cur.executemany(sql, records )
        else :
            raise sqlite3.OperationalError,'Database is not connected. Can not save Data!'
            
    def readTopicList(self, group_id):
        """ 根据group id，返回topic id的列表
        """
        if self.conn:
            cur = self.conn.cursor()
            sql = '''SELECT topic_list FROM GroupInfo WHERE group_id = ?'''
            cur.execute(sql, [(group_id)])
            row = cur.fetchone()
            
            if row is not None:
                str_list = (row[0]).split(",")
                topic_list = []
                for tid in str_list:
                    tid = tid.strip()
                    if tid != "":
                        topic_list.append(tid)
            else:
                print "Error: 找不到group_id = %s 的topic列表" % group_id
                sys.exit()
            return topic_list
        else:
            raise sqlite3.OperationalError,'Database is not connected. Can not save Data!'

    def close(self):
        if self.conn:
            self.conn.close()
        else :
            raise sqlite3.OperationalError, 'Database is not connected.'
