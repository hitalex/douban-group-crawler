# -*- coding: utf-8 -*-

#! /usr/bin/env python
import sys
# 更改python的默认编码为utf-8
reload(sys)
sys.setdefaultencoding('utf-8')

from datetime import datetime
import logging
from lxml import etree # use XPath from lxml
import operator

# for debug
import pdb
from threading import Lock

from patterns import *

log = logging.getLogger('Main.models')

"""
在这里，我将定义会用到的类和数据结构，包括小组、topic和评论。它们之间的关系为：
一个小组包括一些topic，每个评论包括一些评论
注意：
所有的文本全部用UTF-8来编码
"""

class Comment(object):
    """评论的类
    """
    def __init__(self, cid, user_id, pubdate, content, has_quote, quote, quote_content_all, \
            quote_user_id, topic_id, group_id):
        self.cid = cid              # 评论id
        self.user_id = user_id      # 发评论的人的id
        self.pubdate = pubdate      # 发布时间
        self.content = content      # 评论内容，不包括引用评论的内容
        
        self.has_quote = has_quote      # 是否拥有quote
        self.quote = quote          # 引用他人评论, Comment 类
        # 以下两个为临时存储变量，在找到quote comment之后便会删除
        self.quote_content_all = quote_content_all
        self.quote_user_id = quote_user_id
        
        self.topic_id = topic_id    # 所在topic的id
        self.group_id = group_id    # 所在小组的id
        
    def __repr__(self):
        # 默认的换行采用Unix/Linux方式
        if not ('LINE_FEED' in dir()):
            LINE_FEED = u"\n"
        s = u"评论id：" + self.cid + LINE_FEED
        s += u"评论人id：" + self.user_id + LINE_FEED
        s += u"发表时间：" + str(self.pubdate) + LINE_FEED
        if self.quote is not None:
            s += u"引用评论的id：" + self.quote.cid + LINE_FEED
        s += u"内容：" + LINE_FEED + self.content + LINE_FEED
        
        return s
        
    def getSimpleString(self, delimiter):
        """ 获取简单字符串表示
        """
        s = u''
        s += (self.cid + delimiter)
        s += (self.group_id + delimiter)
        s += (self.topic_id + delimiter)
        s += (self.user_id + delimiter)
        s += (str(self.pubdate) + delimiter)
        if self.quote is None:
            s += delimiter
        else:
            s += (self.quote.cid + delimiter)
        s += (self.content) # 直接返回已经中文分词的结果
        
        return s
        
class Topic(object):
    """小组中的某个话题
    """
    def __init__(self, topic_id, group_id):
        self.topic_id = topic_id    # 该topic的id
        self.group_id = group_id    # 所在小组的id
        
        self.user_id = ""           # 发布topic的人的id
        self.user_name = ""         # 用户的昵称
        self.pubdate = ""           # 该topic发布的时间
        self.title = ""             # 该topic的标题
        self.content = ""           # topic的内容
        
        # 在多线程环境下，可能有多个线程同时修改一个Topic的评论列表
        self.lock = Lock()
        self.comment_list = []      # 所有评论的列表, 属于Comment类
        
        self.max_comment_page = 0       # 这个topic具有多少页的评论(包括帖子的首页), init with 0
        # 已经抓取的评论的页面的index
        self.parsedPageIndexSet = set()
        
    def __repr__(self):
        if not ('LINE_FEED' in dir()):
            LINE_FEED = u"\n"
        s = u"话题 id: " + self.topic_id + LINE_FEED
        s += u"小组 id: " + self.group_id + LINE_FEED
        s += u"楼主 id: " + self.user_id + u" 名号: " + self.user_name + LINE_FEED
        s += u"发表时间: " + str(self.pubdate) + LINE_FEED
        s += u"链接：" + self.getSelfLink() + LINE_FEED
        s += u"标题: " + self.title + LINE_FEED
        #s += u"Max number of comment page: " + str(self.max_comment_page) + LINE_FEED
        s += u"帖子内容: " + LINE_FEED + self.content + LINE_FEED + LINE_FEED
        
        # 添加评论内容
        if len(self.comment_list) == 0:
            s += u"（无评论）" + LINE_FEED
        else:
            s += u"评论内容：" + LINE_FEED
            for comment in self.comment_list:
                s += (comment.__repr__() + "\n")
        
        return s
        
        
    def getSimpleString(self, delimiter):
        """ 获取简单字符串表示，不过不包括comment
        """
        s = u""
        s += (self.topic_id + delimiter)
        s += (self.group_id + delimiter)
        s += (self.user_id + delimiter)
        s += (self.title + delimiter)
        s += (str(self.pubdate) + delimiter)
        s += (str(len(self.comment_list)) + delimiter) # number of comments
        # 以后可能还需要记录推荐数和喜欢数等
        s += self.content
        
        return s
        
    def getSelfLink(self):
        """ 获取自身的链接
        """
        url = u"http://www.douban.com/group/topic/" + self.topic_id + "/"
        return url
        
    def parse(self, webPage, isFirstPage):
        """ 从网页中抽取信息，填写类中的字段
        @webPage 网页数据
        @isFirstPage 是否是topic评论的第一页
        返回新添加的comment list
        """
        if isFirstPage:
            return self.extractFirstPage(webPage)
        else:
            return self.extractNonfirstPage(webPage)
            
    def isComplete(self):
        """ 判断评论抓取是否结束
        """
        if self.max_comment_page == 0:
            return False
        
        if len(self.parsedPageIndexSet) < self.max_comment_page:
            return False
        else:
            return True
        
    def extractFirstPage(self, webPage):
        """ 抽取topic首页的topic内容和评论
        返回新添加的comment list
        """
        # 抽取topic首页的内容
        url = "http://www.douban.com/group/topic/" + self.topic_id + "/"
        #print "Reading webpage: " + url
        
        url, pageSource = webPage.getDatas() # pageSource已经为unicode格式        
        page = etree.HTML(pageSource)
        content = page.xpath(u"/html/body/div[@id='wrapper']/div[@id='content']")[0]
        # 找到标题：如果标题太长，那么标题会被截断，原标题则会在帖子内容中显示
        # 如果标题不被截断，则原标题不会在帖子内容中显示
        #pdb.set_trace()
        tmp = page.xpath(u"//table[@class='infobox']//td[@class='tablecc']")
        if len(tmp) == 0:
            # 标题没有被截断
            titlenode = content.xpath("h1")[0]
            self.title = titlenode.text.strip()
        else:
            titlenode = tmp[0]
            self.title = etree.tostring(titlenode, method='text', encoding='utf-8').strip()
        
        if isinstance(self.title, unicode):
            pass
        else:
            self.title = self.title.decode("utf-8")
        
        lz = page.xpath(u"//div[@class='topic-doc']/h3/span[@class='from']/a")[0]
        self.user_name = lz.text.strip()
        url = lz.attrib['href']
        #print url
        match_obj = REPeople.match(url)
        assert(match_obj is not None)
        self.user_id = match_obj.group(1)
        timenode = content.xpath(u"//div[@class='topic-doc']/h3/span[@class='color-green']")[0]
        self.pubdate = datetime.strptime(timenode.text, "%Y-%m-%d %H:%M:%S")
        
        # 帖子内容
        # 帖子中可能包含多种内容，在这里仅处理两种文本和图片链接
        content_node = page.xpath(u"//div[@class='topic-content']")[0]
        children = content_node.getchildren()
        self.content = u""
        for kid in children:
            if kid.tag == 'p':
                self.content += etree.tostring(kid, method='text', encoding='utf-8').strip() + "\n"
            elif kid.tag == 'div' and kid.attrib['class'] == 'topic-figure cc':
                imgnode = kid.xpath("img")[0]
                self.content += u"（图片：" + imgnode.attrib['src'] + "）" + "\n"
            else:
                pass
        
        # 设置本帖子的最大评论页数
        paginator = page.xpath(u"//div[@id='wrapper']//div[@class='paginator']/a")
        if len(paginator) == 0:
            self.max_comment_page = 1 # 如果没有paginator，则只有一页评论
        else:
            max_page = int(paginator[-1].text.strip())
            self.max_comment_page = max_page
        #print "Total comment page: %d" % self.max_comment_page
        
        comments_li = page.xpath(u"//ul[@id='comments']/li")
        # Note: 有可能一个topic下没有评论信息
        newly_added = [] # 本页中新添加的comment
        for cli in comments_li:
            comment = self.extractComment(cli)
            # 为commen_list加锁
            #pdb.set_trace()
            if comment is None:
                continue
            self.lock.acquire()
            self.comment_list.append(comment)
            newly_added.append(comment)
            self.lock.release()
            
        # 在添加评论后对评论按照日期排序
        #sorted(self.comment_list, key=operator.attrgetter('pubdate'), reverse = True)

        # 添加已经抓取的page index
        self.parsedPageIndexSet.add(1)
        
        return newly_added
        
    def extractComment(self, cli):
        # 从comment节点中抽取出Comment结构，并返回Comment对象
        #pdb.set_trace()
        cid = cli.attrib['data-cid']
        
        nodea = cli.xpath("div[@class='reply-doc content']/div[@class='bg-img-green']/h4/a")[0]
        #  如果是已注销的用于，则user_name = '[已注销]'
        user_name = nodea.text
        user_id = self.extractUserID(nodea.attrib['href'])
        pnode = cli.xpath("div[@class='reply-doc content']/p")[0]
        content = unicode(etree.tostring(pnode, method='text', encoding='utf-8')).strip()
        # 发布时间
        strtime = cli.xpath("div[@class='reply-doc content']//span[@class='pubtime']")[0].text
        pubdate = datetime.strptime(strtime, "%Y-%m-%d %H:%M:%S")
        
        # 判断是否有引用其他回复
        quote_id = ""
        quote_node = cli.xpath("div[@class='reply-doc content']/div[@class='reply-quote']")
        has_quote = False
        quote_content_all = None
        quote_user_id = None
        if (quote_node is not None) and (len(quote_node) > 0):
            quote_content_node = quote_node[0].xpath("span[@class='all']")[0]
            quote_content_all = quote_content_node.text.strip()
            url_node = quote_node[0].xpath("span[@class='pubdate']/a")[0]
            url = url_node.attrib['href']
            quote_user_id = self.extractUserID(url)
            has_quote = True
                
        # 这里暂不设置comment所引用的quote，而是只是设立标志位has_quote, 具体quote在抓取完topic之后再确定
        comment = Comment(cid, user_id, pubdate, content, has_quote, None, \
            quote_content_all, quote_user_id, self.topic_id, self.group_id)
        #print "Comment content: ", comment.content
        return comment
        
    def extractUserID(self, url):
        # 从用户链接中抽取出用户id
        match_obj = REPeople.match(url)
        if match_obj is None:
            pdb.set_trace()
        return match_obj.group(1)
        
    def extractNonfirstPage(self, webPage):
        # 抽取topic非首页的内容
        # 如果第一页的评论数不足100，则不可能有第二页评论
        url, pageSource = webPage.getDatas() # pageSource已经为unicode格式  
        if len(self.comment_list) < 100:
            log.warning("It seems there are not engough(100) comments in the first page: \
                Link: %s, Group id: %s, Topic id: %s." % (url, self.group_id, self.topic_id))
        
        page = etree.HTML(pageSource)
        comments_li = page.xpath(u"//ul[@id='comments']/li")
        # Note: 有可能一个topic下没有评论信息
        newly_added = [] # 本页中新添加的comment
        for cli in comments_li:
            comment = self.extractComment(cli)
            # 为commen_list加锁
            self.lock.acquire()
            self.comment_list.append(comment)
            newly_added.append(comment)
            self.lock.release()
        
        # 对评论进行排序
        #sorted(self.comment_list, key=operator.attrgetter('pubdate'), reverse = True)
        
        match_obj = REComment.match(url)
        start = int(match_obj.group(2))
        if start % 100 != 0:
            log.info('链接格式错误：%s' % url)
            
        self.parsedPageIndexSet.add(start / 100 + 1)
        return newly_added
        
    def findPreviousComment(self, end_index, content, user_id):
        # 根据引用的内容和user id，找到引用的评论的链接
        # 比较内容时，不考虑其中的换行符
        import re
        content = re.sub(r'\s', '', content) # remove any white spaces
        for i in range(end_index):
            comment = self.comment_list[i]
            tmp = re.sub(r'\s', '', comment.content)
            if content == tmp and user_id == comment.user_id:
                return comment
                
        # not found, but should be found
        return None
        
    def sortComment(self):
        """ 在完成对该topic的基本信息和所有comment的抽取后，对comment按照时间排序，
        如果某条comment引用之前的评论，则需要设置引用的comment id
        """
        # 对评论进行排序，按照发表时间
        sorted(self.comment_list, key=operator.attrgetter('pubdate'), reverse = True)
        
        comment_count = len(self.comment_list)
        for i in range(comment_count):
            comment = self.comment_list[i]
            if comment.has_quote:
                # 找到引用的回复的comment
                quote_comment = self.findPreviousComment(i, comment.quote_content_all, comment.quote_user_id)
                #释放资源
                comment.quote_content_all = None
                comment.quote_user_id = None
                if quote_comment is None:
                    log.error('Quote comment not found for comment: %s in topic: %s, \
                        in group: \%s' % (cid, self.topic_id, self.group_id))
                    log.error('Quote content: %s' % quote_content_all)
                    log.error('Comment content: %s\n\n' % content)
                else:
                    # 链接找到的comment
                    comment.quote = quote_comment
        
class Group(object):
    """小组类
    主要用于抽取小组本身的信息，比如创建者、创建日期等，以及小组的置顶贴
    注意：这里并不抓取小组的普通帖子
    """
    def __init__(self, group_id):
        self.group_id = group_id            # 小组的id
        self.admin = u""              # 创建小组的user id
        self.pubdate = ""               # 小组创建的时间
        self.desc = u""                 # 小组的介绍
        
        # 小组置顶贴的列表
        self.stick_topic_list = []        # stick topic 的id列表
        
    def __repr__(self):
        """ String representation for class Group
        """
        s = u"小组id: " + self.group_id + "\n"
        s += u"创建者id: " + self.admin + "\n"
        s += u"创建日期: " + str(self.pubdate) + "\n"
        s += u"小组描述: " + self.desc + "\n"
        
        """
        if len(self.stick_topic_list) == 0:
            s += u"（无置顶贴）"
        else:
            s += u"置顶贴ID列表：\n"
            for tid in self.stick_topic_list:
                s += (tid + "\n")
        """
        return s
        
    def getSimpleString(self, delimiter):
        """ 获取简单字符串表示
        """
        s = u''
        s += (self.group_id + delimiter)
        s += (self.admin + delimiter)
        s += (str(self.pubdate) + delimiter)
        s += (self.desc + delimiter)
        s += ','.join(self.stick_topic_list)
        
        return s
        
    def parse(self, webPage):
        """ 从网页中抽取小组信息和置顶贴
        """
        # 默认html是utf-8编码
        #pdb.set_trace()
        url, pageSource = webPage.getDatas()
        
        # 抽取小组信息
        page = etree.HTML(pageSource.decode('utf-8'))
        self.title = page.xpath("/html/head/title")[0].text.strip()
        board = page.xpath("//div[@id='wrapper']//div[@class='group-board']")[0]
        url = board.xpath("p/a")[0].attrib['href']
        self.admin = REPeople.match(url).group(1).strip()
        
        pnode = board.xpath("p")[0]
        strtime = RETime.search(pnode.text).group(0)
        self.pubdate = datetime.strptime(strtime, "%Y-%m-%d")
        
        bdnode = board.xpath("div[@class='group-intro']")[0]
        self.desc = etree.tostring(bdnode, method='text', encoding='utf-8').strip()
        self.desc = self.desc.decode('utf-8').strip()
        
        self.extractSticTopic(webPage)
        
    def extractSticTopic(self, webPage):
        """ 抓取小组的置顶topic id列表
        """
        url, pageSource = webPage.getDatas()
        if not isinstance(pageSource, unicode):
            # 默认页面采用UTF-8编码
            page = etree.HTML(pageSource.decode('utf-8'))
        else:
            page = etree.HTML(pageSource)
        stickimg = page.xpath(u"div[@class='group-topics']//img[@alt='[置顶]']")
        # 可能一个小组中没有置顶贴，此时stickmig为空
        for imgnode in stickimg:
            titlenode = imgnode.getparent().getparent()
            href = titlenode.xpath("a")[0].attrib['href']
            # 加入到topic列表中
            #print "Add stick post: ", href
            match_obj = RETopic.match(href)
            self.stick_topic_list.append(match_obj.group(1))
        
        
if __name__ == "__main__":
    #f = open("./testpage/求掀你的英语怎样从烂到无底洞到变强人的！！！.html")
    f = open(u"./testpage/标题：【手机行业】你不得不知道的手机内幕(非专业....html", "r")
    #f = open("./testpage/掀起你的内幕来™┏(゜ω゜)=☞小组.html", "r")
    strfile = f.read()
    f.close()
    topic = Topic('31195872', 'insidestory', strfile, u"")
    print topic
    #g = Group("http://www.douban.com/group/insidestory/", strfile)
    #print g

