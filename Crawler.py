import sys
import configparser
import json
import logging
import os
import pprint
from selenium import webdriver
from selenium.common.exceptions import NoSuchElementException
from Logger import Logger

class Crawler:

    def __init__(self):
        self.logger = Logger('Crawler')
        self.Setting()
        self.LoadDriver()
        self.logger.logger.info('Finish initializing Crawler')
         
    def Setting(self):
        self.config = configparser.ConfigParser()
        # print(os.getcwd())
        with open('Config.ini') as file:
            self.config.readfp(file)

        self.browserLocation = self.config.get('Options','Chrome_Location')
        self.userData = self.config.get('Options','Chrome_UserData')
        # self.logPath = self.config.get('Options','Log_Path')
        
        # formatter = logging.Formatter('[%(name)-12s %(levelname)-8s] %(asctime)s - %(message)s')
        # self.logger=logging.getLogger(__class__.__name__)
        # self.logger.setLevel(logging.DEBUG)
        
        # if not os.path.isdir(self.logPath):
        #     os.mkdir(self.logPath)

        # fileHandler = logging.FileHandler(self.logPath+'log.txt')
        # fileHandler.setLevel(logging.INFO)
        # fileHandler.setFormatter(formatter)

        # streamHandler = logging.StreamHandler()
        # streamHandler.setLevel(logging.DEBUG)
        # streamHandler.setFormatter(formatter)

        # self.logger.addHandler(fileHandler)
        # self.logger.addHandler(streamHandler)

        # self.logger.info('Finish Setting')

    def LoadDriver(self):
        self.logger.logger.info('Driver Loading')
        self.options = webdriver.ChromeOptions()
        self.options.binary_location = self.browserLocation
        #self.options.add_argument(self.userData)
        # self.options.add_argument('headless')
        #self.options.add_argument(self.userData)
        self.options.add_argument('incognito')
        self.driver = webdriver.Chrome(chrome_options=self.options)
        self.logger.logger.info('Finish loading Driver')

    def __del__(self):
        try:
            self.driver.close()
            self.driver.quit()
        except:
            pass

def main():
    logging.info(__name__)
    parser = Crawler()

if __name__ == '__main__':
    main()

