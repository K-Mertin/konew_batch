"""
loggers
"""
import os
import logging
import configparser
import datetime

class Logger:

    def __init__(self, name):
        self.name = name
        self.Setting()     

    def Setting(self):

        self.config = configparser.ConfigParser()

        with open('Config.ini') as file:
            self.config.readfp(file)

        self.logPath = self.config.get('Options','Log_Path')
        self.logLevel = self.config.get('Options','Log_Level')
        
        formatter = logging.Formatter('[%(name)-12s %(levelname)-8s] %(asctime)s - %(message)s')
    
        self.logger=logging.getLogger(self.name)

        self.logger.setLevel(logging.getLevelName(self.logLevel))
        
        if not os.path.isdir(self.logPath):
            os.mkdir(self.logPath)

    
        fileHandler = logging.FileHandler(self.logPath+ datetime.datetime.now().strftime("%Y%m%d")+ '_' +self.name+'.log')
        fileHandler.setLevel(logging.INFO)
        fileHandler.setFormatter(formatter)

        streamHandler = logging.StreamHandler()
        streamHandler.setLevel(logging.DEBUG)
        streamHandler.setFormatter(formatter)

        self.logger.addHandler(fileHandler)
        self.logger.addHandler(streamHandler)

        self.logger.info('Finish Log Setting')
    
    def testFunc(self):
        self.logger.info('tests')

def main():
    logger = Logger('test')

if __name__ == '__main__':
    main()