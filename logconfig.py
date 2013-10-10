#encoding=utf8

# 日志配置文件
import logging

def getLogger(logFile, logLevel = 4):
    '''配置logging的日志文件以及日志的记录等级'''
    LEVELS={
        1:logging.CRITICAL, 
        2:logging.ERROR,
        3:logging.WARNING,
        4:logging.INFO,
        5:logging.DEBUG,#数字越大记录越详细
    }
    
    logger = logging.getLogger(logFile) # 用logFile代替logger的名称
    hdlr = logging.FileHandler(logFile)
    formatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s')
    hdlr.setFormatter(formatter)
    logger.addHandler(hdlr)
    
    logger.setLevel(LEVELS.get(logLevel))
     
    return logger
