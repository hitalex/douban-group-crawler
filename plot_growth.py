#coding=utf8

"""
从已经抓取的topic中找到那些较为popular的帖子，将其评论数(看作时间序列)增长趋势画出来
"""
import codecs
from datetime import datetime, timedelta
import math

from matplotlib.dates import date2num
import matplotlib.pyplot as plt
import pandas as pd

from utils import load_id_list

threshold = 300 # 考虑的最少评论数

def plot_popularity(date_list):
    """ 根据评论的时间列表画出增长示意图
    """
    x = date2num(date_list)
    count = len(date_list)
    
    y = range(count)
    for i in range(count):
        y[i] = math.log(y[i]+1)
    
    plt.figure()
    plt.plot(x, y)
    
    plt.show()
    
    
def plot_granularity_popularity(date_list, freq = 'D'):
    """ 以不同的时间粒度为单位画出流行度曲线
    date_list[0] is the topic publishing date
    Note: 这里假设date_list中的时间是按照时间前后排序后的
    freq 的取值有：{'S', 'M', 'H', 'D', 'M', 'Y', 'W'}
    """
    start = date_list[0]
    end = date_list[-1]
    
    delta = timedelta(days=30*6) # 构造一个时间差为半年的timedelta对象
    if end > start + delta:
        end = start + delta # 只考虑半年内的评论
    # create a time series with specified freq
    ts = pd.Series(0, pd.date_range(start, end, freq = freq))
    index = 0
    
    for date in date_list:
        if date > ts.index[index]:
            index = index + 1
            if index >= len(ts):
                break

        ts[index] = ts[index] + 1
        
    print 'Number of %s(s): %d' % (freq, len(ts))
    # caculate the cumulative sum
    cum_count = ts.cumsum()
    
    for i in range(len(ts)):
        ts[i] = math.log(ts[i] + 1)
        cum_count[i] = math.log(cum_count[i])
        
    plt.figure()
    #plt.plot(ts, color='blue', hold=True, ls=':')
    #plt.plot(cum_count, color='red', ls='--')
    ts.plot(color='blue', ls='-')
    cum_count.plot(color='red', ls='--')
    
    plt.show()

def main(group_id):
    # 读取topic id list
    path = 'data/' + group_id + '/' + group_id + '-TopicList.txt'
    topic_id_list = load_id_list(path)
    #topic_id_list = ['34029324']
    
    for topic_id in topic_id_list:
        path = 'data/' + group_id + '/' + topic_id + '-content.txt'
        topic_pubdate = None
        try:
            with codecs.open(path, 'r', 'utf-8') as f:
                content = f.read()
                seg_list = content.split('[=]')
                num_comment = int(seg_list[5])
                # topic publishing date
                topic_puddate = datetime.strptime(seg_list[4], '%Y-%m-%d %H:%M:%S')
                if num_comment < threshold:
                    continue
        except IOError:
            continue
            
        path = 'data/' + group_id + '/' + topic_id + '-comment.txt'
        f = codecs.open(path, 'r', 'utf-8')
        date_list = [topic_puddate]
        row = ''
        for line in f:
            if line != '[*ROWEND*]\n':
                row = row + line
            else:
                seg_list = row.split('[=]')
                date = seg_list[4]
                date = datetime.strptime(date, '%Y-%m-%d %H:%M:%S')
                date_list.append(date)
                row = ''
            
        sorted(date_list, reverse = True)
        
        count = len(date_list)
        print 'topic id:', topic_id
        print 'Number of comments: %d\n' % (count-1)
        
        #plot_popularity(date_list)
        plot_granularity_popularity(date_list, freq = 'D')

if __name__ == '__main__':
    import sys
    group_id = sys.argv[1]
    main(group_id)
