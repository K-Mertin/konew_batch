from Crawler import Crawler
from DataAccess import DataAccess
from Logger import Logger
import re, math
import time, os

class LawBankParser:

    def __init__(self, driver, dataAccess, logger):
        self.driver = driver
        self.dataAccess = dataAccess
        self.logger = logger
    
    def PageAnalysis(self, searchKeys, referenceKeys):
        self.logger.logger.info('start PageAnalysis')
        try:
            document = {}
            title = self.driver.find_element_by_css_selector('.Table-List tr:nth-child(1)> td:nth-child(2)').text
            date = self.driver.find_element_by_css_selector('.Table-List tr:nth-child(2)> td:nth-child(2)').text
            reason = self.driver.find_element_by_css_selector('.Table-List tr:nth-child(3)> td:nth-child(2)').text
            content = self.driver.find_element_by_css_selector('.Table-List tr:nth-child(5)> td:nth-child(1)').text
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
            if re.search(key,content):
                tags.append(key)
        
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
            keyword.send_keys(searchKey)

            form = self.driver.find_element_by_id('form1')
            form.submit()

            return True
        except:
            self.logger.logger.error('error search Key')
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
                print(lists)
                # self.driver.find_elements_by_css_selector
                for li in lists:
                    if not li.text.endswith('0'):
                        self.totalCount +=  int(li.text.split(' ')[1])#   int(re.search( r'\(.*\)', li.text).group().replace('(','').replace(')',''))
                        self.courts.append(li.find_element_by_css_selector('a').get_attribute('href'))            
        except  Exception as e: 
            print(e)


        
    def processIter(self, searchKeys, referenceKeys, requestId):
        for c in self.courts:
            time.sleep(0.5) 
            documents = []
            self.driver.get(c)
            self.driver.find_elements_by_css_selector('#table3 a')[0].click()
            documents.append(self.PageAnalysis(searchKeys, referenceKeys))
            nextPage = self.driver.find_element_by_css_selector('tbody > tr:nth-child(1) > td:nth-child(2) > a:nth-child(3)')

            while nextPage.is_displayed():
                nextPage.click()
                documents.append(self.PageAnalysis(searchKeys, referenceKeys))
                nextPage = self.driver.find_element_by_css_selector('tbody > tr:nth-child(1) > td:nth-child(2) > a:nth-child(3)')

            self.dataAccess.insert_documents(str(requestId),documents)


    def processModifiedKey(self):
        #process modified referenceKey
        requests = self.dataAccess.get_modified_requests()
        
        if requests.count()>0 :
            for request in requests:
                requestId = request['requestId']
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

    def processNewRequest(self):
        # process new requests
        requests = self.dataAccess.get_created_requests()

        if requests.count()>0 :
            
            for request in requests:
                requestId = request['requestId']
                searchKeys = list(map(lambda x : x['key'], request['searchKeys']))
                referenceKeys = request['referenceKeys']
                _id = request['_id']

                for searchKey in searchKeys:
                    print(searchKey)
                    if self.Search(searchKey):
                        print('get data')
                        self.getCourts()
                        self.dataAccess.processing_requests(_id,searchKey,self.totalCount)
                        self.processIter(searchKeys,referenceKeys,requestId)

                dataAccess.finish_requests(_id)
                
                print(request)

    def processProcessingKey(self):
        # process new requests
        requests = self.dataAccess.get_processing_requests()

        if requests.count()>0 :
            
            for request in requests:
                requestId = request['requestId']
                searchKeys = list(map(lambda x : x['key'], request['searchKeys']))
                referenceKeys = request['referenceKeys']
                _id = request['_id']

                self.dataAccess.remove_all_documents(requestId)

                for searchKey in searchKeys:
                    print(searchKey)
                    self.Search(searchKey)
                    self.getCourts()
                    self.dataAccess.processing_requests(_id,searchKey,self.totalCount)
                    self.processIter(searchKeys,referenceKeys,requestId)

                self.dataAccess.finish_requests(_id)
                
                print(request)    

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

    while( not os.path.exists('process.stop')):
        try:
            parser.processModifiedKey()
            parser.processNewRequest()
            parser.processProcessingKey()
        except Exception as e:
            logger.logger.error(e)
        time.sleep(60)

if __name__ == '__main__':
    main()
