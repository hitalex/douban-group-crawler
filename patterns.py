#coding:utf8

# 这里存放所有用于匹配的正则表达式

import re

# 用户的主页链接, group(1)为用户id
REPeople = re.compile("^http://www.douban.com/group/people/([0-9, a-z, A-Z, _, \-, \.]+)/$")

# 小组首页链接, group(1)为小组id
REGroup = re.compile("^http://www.douban.com/group/([0-9, a-z, A-Z, _, \-, \.]+)/$")

# Topic页面链接, group(1)为topic id
RETopic = re.compile("^http://www.douban.com/group/topic/([0-9]+)/$")

# 小组讨论列表的链接, group(1)为小组id，group(2)为start
REDiscussion = re.compile("^http://www.douban.com/group/([0-9,a-z,A-Z,\-,_,\.]+)/discussion\?start=([0-9]+)$")

# topic评论页面链接，group(1)为topic id，group(2)为start page
REComment = re.compile("^http://www.douban.com/group/topic/([0-9]+)/\?start=([0-9]+)$")

# 时间模板, 格式如：2012-04-12
RETime = re.compile("[0-9]{4}\-[0-9]{2}\-[0-9]{2}")

# URL 模板,包括了普通URL和图片的http链接
REURL = re.compile(r'((http|https|ftp)://([\w\-]+\.)+[\w\-]+(/[\w\-\./\?%&\=]*)?)')
REIMGURL = re.compile(r'(http://img3\.douban\.com/.*\.jpg)')
