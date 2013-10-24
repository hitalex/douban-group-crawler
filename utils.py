#encoding=utf8

def seg_chinese(chinese_str):
    """中文分词
    Note: 目前采用jieba分词. jieba项目主页：https://github.com/fxsjy/jieba
    """
    import jieba
    seg_list = jieba.cut(chinese_str)
    return " ".join(seg_list)
    
def is_between(now, start_date, end_date):
    """ 判断给定的时间是否在起止时间内
    """
    from datetime import datetime
    
    if now >= start_date and now < end_date:
        return True
    else:
        return False
        
def load_id_list(file_path):
    """ 从文件内导入所有的id，每行一个，返回这些id的list
    """
    f = open(file_path, 'r')
    id_list = []
    for line in f:
        line = line.strip()
        if line != '':
            id_list.append(line)
    f.close()
    
    return id_list
