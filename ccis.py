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

        try:
            soup = BeautifulSoup(content.text, "html.parser")
            title = soup.select('#Label_dinffdate')[0].text
            # print(title)
            lists= soup.select('#GridView1  > tr')
        except Exception as e:
            self.logger.logger.error('解析網頁錯誤')
            self.logger.logger.error(str(e))

        for idx, item in enumerate(lists):
            if item.select('td:nth-of-type(2)'):
                try:
                    relation = {
                        'reason':'中華徵信拒往'+title[0:10],
                        'subjects': [],
                        'objects': [{
                            'idNumber': None,
                            'name': 'NA',
                            'relationType': ['NA'],
                            'memo': []
                        }],
                        'user': 'system'
                    }
                    subject = {}
                    subject['idNumber'] = item.select('td:nth-of-type(2)')[0].text
                    subject['name'] = item.select('td:nth-of-type(4)')[0].text 
                    subject['memo'] = [title]
                    for i in item.find(id='GridView1_GridView_CmpData_'+str(idx-1)).select('tr'):
                        if i.find('input'):
                            subject['memo'] = subject['memo'] + [i.findAll('td')[2].text +'-' +i.find('input').get('value')]

                    
                    relation['subjects'].append(subject)
                    # print(relation)
                    # print('---------------------------------------------------------------------------------')  
                    self.importRelation(relation)
                except Exception as e:
                    self.logger.logger.error('資料匯入錯誤')
                    self.logger.logger.error(str(e))
        
       

    def importRelation(self, relation):
        idNumber = relation['subjects'][0]['idNumber']

        ret = self.dataAccess.get_relation(idNumber)
        # print(ret)
        if ret:
            ret['reason'] = relation['reason']
            ret['subjects'][0]['name'] = relation['subjects'][0]['name']

            if len(ret['subjects'][0]['memo']) > 0:
                for memo in relation['subjects'][0]['memo']:
                    if memo not in  ret['subjects'][0]['memo']:
                        ret['subjects'][0]['memo'].append(memo)
            else:
                ret['subjects'][0]['memo'] =  relation['subjects'][0]['memo']
            
            objIdList = list(map(lambda x:x['idNumber'], ret['objects']))
            # print (objIdList)

            for obj in relation['objects']:
                if obj['idNumber'] and obj['idNumber'] in objIdList:
                    for ret_obj in ret['objects']:
                        if ret_obj['idNumber'] == obj['idNumber']:
                            
                            ret_obj['name'] = obj['name']

                            for memo in obj['memo']:
                                if memo not in ret_obj['memo']:
                                    ret_obj['memo'].append(memo)

                            for relationType in obj['relationType']:
                                if relationType not in ret_obj['relationType']:
                                    ret_obj['relationType'].append(relationType)
                else:
                    ret['objects'].append(obj)

            
            # ret['objects'] = relation['objects']
            ret['_id'] = str(ret['_id'])
            ret['user'] = relation['user']
            
            ret = self.dataAccess.update_relation(ret, '')
        else:
            ret = self.dataAccess.insert_relation(relation)


    
    def process(self):
        try:
            currentUrl = 'https://smart.ccis.com.tw/CCHS/RPT/sRptWeek.aspx'
            content = requests.get(currentUrl)
            self.parserHtml(content)
        except Exception as e:
            self.logger.logger.error('取得資料錯誤')
            self.logger.logger.error(str(e))


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


if __name__ == '__main__':
    main()
