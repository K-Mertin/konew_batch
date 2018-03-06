from Logger import Logger
from dataAccess.relationAccess import relationAccess
import time, os
import configparser
import requests
from bs4 import BeautifulSoup
import sys

class CcisParser:

    def __init__(self, dataAccess, logger):
        self.Setting()
        self.dataAccess = dataAccess
        self.logger = logger
        self.headers = {'User-Agent':'Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/63.0.3239.132 Safari/537.36'}
                    
    def Setting(self):
        self.config = configparser.ConfigParser()
        with open('Config.ini') as file:
            self.config.readfp(file)

        self.timeInterval = int(self.config.get('Options','Time_Interval'))
        self.stopFile = self.config.get('Options','Stop_File')
        self.dataPath = self.config.get('Options','Data_Path')

    def parserHtml(self, content):
        soup = BeautifulSoup(content.text, "html.parser")
        title = soup.select('#Label_dinffdate')[0].text

        ret = {}
        print(title)

        lists= soup.select('#GridView1  > tr')
        for idx, item in enumerate(lists):
            if item.select('td:nth-of-type(2)'):
                idNumber = item.select('td:nth-of-type(2)')[0].text
                name = item.select('td:nth-of-type(4)')[0].text 
                memo = []
                for i in item.find(id='GridView1_GridView_CmpData_'+str(idx-1)).select('tr'):
                    if i.find('input'):
                        memo = memo + [i.findAll('td')[2].text +'-' +i.find('input').get('value')]
                print(idNumber)
                print(name)
                print(memo)
                print('---------------------------------------------------------------------------------')  
    
    def process(self):
        currentUrl = 'https://smart.ccis.com.tw/CCHS/RPT/sRptWeek.aspx'
        content = requests.get(currentUrl)
        self.parserHtml(content)

def main():
    logger = Logger('ccis')
    logger.logger.info('start process')

    try:
        dataAccess = relationAccess()
        parser = CcisParser(dataAccess, logger)

    except Exception as e:
        logger.logger.error(e)
        return
   
    logger.logger.info('Finish initializing Batch')

    parser.process()

    logger.logger.info('Batch stop')