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
        try:
            with open('Config.ini') as file:
                self.config.readfp(file)
            
            self.source = self.config.get('Options','Selenium_Source')

            if self.source == 'LOCAL':
                self.browserLocation = self.config.get('Options','Chrome_Location')
                self.userData = self.config.get('Options','Chrome_UserData')
            else:
                self.seleniumService = self.config.get('Options','Selenium_Service')
        except Exception as e:
            self.logger.logger.error(e)
            raise(e)

    def LoadDriver(self):
        self.logger.logger.info('Driver Loading')
        self.options = webdriver.ChromeOptions()
        # self.options.add_argument('headless')
        self.options.add_argument('incognito')
        try:
            if self.source == 'LOCAL':
                #self.options.add_argument(self.userData)
                self.options.binary_location = self.browserLocation
                self.driver = webdriver.Chrome('./chromedriver.exe',chrome_options=self.options)
            else:
                self.driver = webdriver.Remote(command_executor='http://127.0.0.1:4444/wd/hub',desired_capabilities=self.options.to_capabilities())
        except Exception as e:
            self.logger.logger.error(e)
            raise(e)

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

