from Crawler import Crawler
from DataAccess import DataAccess
from Logger import Logger
import re, math
import time, os
import configparser

class LawBankParser:

    def __init__(self, driver, dataAccess, logger):
        self.Setting()
        self.driver = driver
        self.dataAccess = dataAccess
        self.logger = logger

    def Setting(self):
        self.config = configparser.ConfigParser()
        with open('Config.ini') as file:
            self.config.readfp(file)

        self.timeInterval = int(self.config.get('Options','Time_Interval'))
        self.stopFile = self.config.get('Options','Stop_File')
        self.dataPath = self.config.get('Options','Data_Path')
    
    def PageAnalysis(self, searchKeys, referenceKeys):
        # self.logger.logger.info('start PageAnalysis')
        try:
            document = {}
            title = self.driver.find_element_by_css_selector('.Table-List tr:nth-child(1)> td:nth-child(2)').text
            date = self.driver.find_element_by_css_selector('.Table-List tr:nth-child(2)> td:nth-child(2)').text
            reason = self.driver.find_element_by_css_selector('.Table-List tr:nth-child(3)> td:nth-child(2)').text
            content = self.driver.find_element_by_css_selector('.Table-List tr:nth-child(5)> td:nth-child(1)').text.replace('\n','')
            url = self.driver.current_url
        except:
            self.logger.logger.error('error parser document')

        document['title'] = title
        document['date'] = date if len(date) == 9 else '0'+date
        document['tags'] = [reason]
        document['tags'].append(title.split(' ')[0])
        document['tags'].append(re.sub('\[.*\]', '', title, count=0, flags=0)[-5:-1])
        document['searchKeys'] = self.ContentAnalysis(content, searchKeys)
        document['referenceKeys'] = self.ContentAnalysis(content, referenceKeys)
        document['source'] = url
        document['content'] = content

        return document

    def ContentAnalysis(self, content, keys):
        tags = []

        for key in keys:
            singleKeys=key.replace('+',' ').replace('&',' ').replace('(',' ').replace(')',' ').replace('-',' ').split(' ')
            # print(singleKeys)
            for singleKey in singleKeys:
                if len(singleKey) >0:
                    if re.search(singleKey,content):
                        tags.append(singleKey)
        
        return tags

    def Search(self, searchKey):
        self.logger.logger.info('Search Key :' + searchKey)
        try:
            self.driver.get('http://fyjud.lawbank.com.tw/index.aspx')

            elements = self.driver.find_elements_by_css_selector('input[type=checkbox]')
            # for element in elements:
            #     if not element.is_selected():
            #         element.click()
            keyword = self.driver.find_element_by_id('kw')
            keyword.clear()
            # time.sleep(10)
            keyword.send_keys(searchKey)

            form = self.driver.find_element_by_id('form1')
            form.submit()
            # time.sleep(10)
            return True
        except Exception as e:
            self.logger.logger.error(e)
            return False
    
    def getCourts(self):
        try:
            self.driver.switch_to_default_content()
            self.driver.switch_to_frame('menuFrame')
            courtGroups = self.driver.find_elements_by_class_name('court_group')
            
            
            self.courts = []
            self.totalCount = 0
        
            for courtGroup in courtGroups:
                lists = courtGroup.find_elements_by_css_selector('li')
                # print(lists)
                # self.driver.find_elements_by_css_selector
                for li in lists:
                    if not li.text.endswith(' 0'):
                        # print( int(li.text.split(' ')[1]))
                        self.totalCount +=  int(li.text.split(' ')[1])#   int(re.search( r'\(.*\)', li.text).group().replace('(','').replace(')',''))
                        self.courts.append(li.find_element_by_css_selector('a').get_attribute('href'))      
            self.logger.logger.info('totalNo:'+str(self.totalCount))      
        except  Exception as e: 
            self.logger.logger.error(e)


        
    def processIter(self, searchKeys, referenceKeys, requestId):
        processCount = 0
        for c in self.courts:
            time.sleep(0.5) 
            documents = []
            self.driver.get(c)
            self.driver.find_elements_by_css_selector('#table3 a')[0].click()
            documents.append(self.PageAnalysis(searchKeys, referenceKeys))
            nextPage = self.driver.find_element_by_css_selector('tbody > tr:nth-child(1) > td:nth-child(2) > a:nth-child(3)')
            processCount += 1

            if self.totalCount >10 and processCount%(int(self.totalCount/10))==0 :
                    self.logger.logger.info(str(processCount)+'_'+str(processCount*100/self.totalCount)+'%')


            while nextPage.is_displayed():
                time.sleep(0.1)
                nextPage.click()
                documents.append(self.PageAnalysis(searchKeys, referenceKeys))
                nextPage = self.driver.find_element_by_css_selector('tbody > tr:nth-child(1) > td:nth-child(2) > a:nth-child(3)')
                processCount += 1
                
                if self.totalCount >10 and processCount%int(self.totalCount/10)==0:
                    self.logger.logger.info(str(processCount)+'_'+str(processCount*100/self.totalCount)+'%')

            self.dataAccess.insert_documents(str(requestId),documents)


    def processModifiedKey(self):
        #process modified referenceKey
        requests = self.dataAccess.get_modified_requests()
        
        if requests.count()>0 :
            for request in requests:
                try:
                    requestId = request['requestId']
                    self.logger.logger.info('processModifiedKey:'+requestId)
                    referenceKeys = request['referenceKeys']
                    _id = request['_id']

                    pageSize = 10
                    totalCount = self.dataAccess.get_documents_count(str(requestId))
                    totalPages = math.ceil(totalCount/pageSize)

                    for i in range(1,totalPages+1):
                        documents = self.dataAccess.get_allPaged_documents(str(requestId),pageSize,i)
                        for doc in documents:
                            self.dataAccess.update_document_reference(str(requestId),doc['_id'],self.ContentAnalysis(doc['content'], referenceKeys))
                            
                    self.dataAccess.finish_requests(_id)
                    self.logger.logger.info('finish_requests:'+requestId)
                except Exception as e:
                    self.logger.logger.error(e)

    def processNewRequest(self):
        # process new requests
        requests = self.dataAccess.get_created_requests()

        if requests.count()>0 :
            for request in requests:
                try:
                    requestId = request['requestId']
                    self.logger.logger.info('processNewRequest:'+requestId)
                    searchKeys = list(map(lambda x : x['key'], request['searchKeys']))
                    referenceKeys = request['referenceKeys']
                    _id = request['_id']

                    for searchKey in searchKeys:
                        # print(searchKey)
                        time.sleep(0.5)
                        if self.Search(searchKey):
                            # print('get data')
                            self.getCourts()
                            self.dataAccess.processing_requests(_id,searchKey,self.totalCount)
                            self.processIter(searchKeys,referenceKeys,requestId)
                        else:
                            raise Exception()

                    self.dataAccess.finish_requests(_id)
                    self.logger.logger.info('finish_requests:'+requestId)
                except Exception as e:
                    self.logger.logger.error(e)
                # print(request)

    def processProcessingKey(self):
        # process new requests
        requests = self.dataAccess.get_processing_requests()

        if requests.count()>0 :
            
            for request in requests:
                try:
                    requestId = request['requestId']
                    self.logger.logger.info('processProcessingKey:'+requestId)
                    searchKeys = list(map(lambda x : x['key'], request['searchKeys']))
                    referenceKeys = request['referenceKeys']
                    _id = request['_id']

                    self.dataAccess.remove_all_documents(requestId)

                    for searchKey in searchKeys:
                        # print(searchKey)
                        if self.Search(searchKey):
                            # print('get data')
                            self.getCourts()
                            self.dataAccess.processing_requests(_id,searchKey,self.totalCount)
                            self.processIter(searchKeys,referenceKeys,requestId)
                        else:
                            raise Exception()

                    self.dataAccess.finish_requests(_id)
                    self.logger.logger.info('finish_requests:'+requestId)
                except Exception as e:
                    self.logger.logger.error(e)
                # print(request)
    
    def process(self):
        self.logger.logger.info('Start Process')

        while( not os.path.exists(self.dataPath+'process.stop')):
            try:
                # print("processing")
                self.processModifiedKey()
                self.processNewRequest()
                self.processProcessingKey()
            except Exception as e:
                self.logger.logger.error(e)
            time.sleep(self.timeInterval)

def main():
    logger = Logger('lawbank')
    logger.logger.info('start process')

    try:
        dataAccess = DataAccess()
        crawler = Crawler()
        parser = LawBankParser(crawler.driver, dataAccess, logger)
    except Exception as e:
        logger.logger.error(e)
        return
   
    logger.logger.info('Finish initializing Batch')

    parser.process()

    logger.logger.info('Batch stop')


if __name__ == '__main__':
    main()
