from Crawler import Crawler
from DataAccess import DataAccess
from Logger import Logger
import re, math
import time, os
import configparser
import requests
from bs4 import BeautifulSoup
import sys

class LawBankParser:

    def __init__(self, driver, dataAccess, logger):
        self.Setting()
        self.driver = driver
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
    
    def PageAnalysis(self,currentUrl ,content, searchKeys, referenceKeys):
        # self.logger.logger.info('start PageAnalysis')
        try:
            document = {}
            soup = BeautifulSoup(content.text, "html.parser")
            title = soup.select('.Table-List tr > td:nth-of-type(2)')[0].text
            date = soup.select('.Table-List tr > td:nth-of-type(2)')[1].text
            reason = soup.select('.Table-List tr > td:nth-of-type(2)')[2].text
            content = soup.select('.Table-List pre')[0].text
            # title = self.driver.find_element_by_css_selector('.Table-List tr:nth-child(1)> td:nth-child(2)').text
            # date = self.driver.find_element_by_css_selector('.Table-List tr:nth-child(2)> td:nth-child(2)').text
            # reason = self.driver.find_element_by_css_selector('.Table-List tr:nth-child(3)> td:nth-child(2)').text
            # content = self.driver.find_element_by_css_selector('.Table-List tr:nth-child(5)> td:nth-child(1)').text.replace('\n','')
            url = currentUrl

            if content == '由於裁判書全文大於 1 M，請按此下載檔案。':
                # print(content)
                downLink = soup.select('.Table-List pre')[0].find('a')['href']
                f = requests.get('http://fyjud.lawbank.com.tw/'+downLink, headers = self.headers)
                content = f.content.decode('ANSI')
                # print(content)

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
        # if len(keys) > 0:
        for key in keys:
            # print(key)
            singleKeys=key.replace('+',' ').replace('&',' ').replace('(',' ').replace(')',' ').replace('-',' ').split(' ')
            # print(singleKeys)
            for singleKey in singleKeys:
                if len(singleKey) >0:
                    pattern = ''
                    for index in range(len(singleKey)):
                        if index == len(singleKey)-1:
                            pattern = pattern+singleKey[index]
                        else:
                            pattern = pattern+singleKey[index] + '\s*'

                    if re.search(pattern,content):
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

            year1 = self.driver.find_element_by_id('dy1')
            year1.clear()

            year2 = self.driver.find_element_by_id('dy2')
            year2.clear()

            mon1 = self.driver.find_element_by_id('dm1')
            mon1.clear()

            mon2 = self.driver.find_element_by_id('dm2')
            mon2.clear()
            # time.sleep(10)
            print(searchKey)
            keyword.send_keys(searchKey)

            form = self.driver.find_element_by_id('form1')
            form.submit()
            # time.sleep(10)
            return True
        except Exception as e:
            self.logger.logger.error(e)
            return False
    
    def SearchYear(self, searchKey, courtCode, startYear = 85,  endYear = 107):  

        adder = int((endYear-startYear)/2)

        while(startYear<=endYear):
            self.driver.get('http://fyjud.lawbank.com.tw/index.aspx')
            elements = self.driver.find_elements_by_css_selector('input[type=checkbox]')

            for element in elements:
                if element.get_attribute('value').lower()== str(courtCode).lower():
                    if not element.is_selected():
                            element.click()
                else:
                    if element.is_selected():
                            element.click()
            year1 = self.driver.find_element_by_id('dy1')
            year1.clear()
            year1.send_keys(startYear)

            year2 = self.driver.find_element_by_id('dy2')
            year2.clear()
            year2.send_keys(startYear+adder)

            mon1 = self.driver.find_element_by_id('dm1')
            mon1.clear()

            mon2 = self.driver.find_element_by_id('dm2')
            mon2.clear()

            keyword = self.driver.find_element_by_id('kw')
            keyword.clear()

            searchKey=searchKey
            keyword.send_keys(searchKey)

            form = self.driver.find_element_by_id('form1')
            form.submit()

            self.getCourts(searchKey, startYear,startYear+adder)
            startYear= startYear+adder +1
            time.sleep(0.5)

    def SearchMonth(self, searchKey, courtCode, year, startMonth = 1,endMonth = 12):
        
        adder = int((endMonth-startMonth)/2)

        while(startMonth<=endMonth):
            self.driver.get('http://fyjud.lawbank.com.tw/index.aspx')
            elements = self.driver.find_elements_by_css_selector('input[type=checkbox]')

            for element in elements:
                if element.get_attribute('value').lower()== str(courtCode).lower():
                    if not element.is_selected():
                            element.click()
                else:
                    if element.is_selected():
                            element.click()
            year1 = self.driver.find_element_by_id('dy1')
            year1.clear()
            year1.send_keys(year)

            year2 = self.driver.find_element_by_id('dy2')
            year2.clear()
            year2.send_keys(year)

            mon1 = self.driver.find_element_by_id('dm1')
            mon1.clear()
            mon1.send_keys(startMonth)

            mon2 = self.driver.find_element_by_id('dm2')
            mon2.clear()
            mon2.send_keys(startMonth+adder)

            keyword = self.driver.find_element_by_id('kw')
            keyword.clear()

            searchKey=searchKey
            keyword.send_keys(searchKey)

            form = self.driver.find_element_by_id('form1')
            form.submit()

            self.getCourts(searchKey, year, year, startMonth,startMonth+adder)
            startMonth = startMonth + adder +1
            time.sleep(0.5)


    def getCourts(self, searchKey, startYear=0, endYear=999, startMonth=0, endMonth=99):
        try:
            self.driver.switch_to_default_content()
            self.driver.switch_to_frame('menuFrame')
            courtGroups = self.driver.find_elements_by_class_name('court_group')
            yearList = []
            monthList = []
            
            if startYear == 0 :
                self.courts = []
                self.totalCount = 0    
        
            for courtGroup in courtGroups:
                lists = courtGroup.find_elements_by_css_selector('li')
                # print(lists)
                # self.driver.find_elements_by_css_selector
                for li in lists:
                    if not li.text.endswith(' 0'):
                        # print( int(li.text.split(' ')[1]))
                        if startYear == 0 :
                            self.totalCount +=  int(li.text.split(' ')[1])#   int(re.search( r'\(.*\)', li.text).group().replace('(','').replace(')',''))
                        if int(li.text.split(' ')[1]) > 999:
                            if endYear - startYear >=1:
                                yearList.append(li.get_attribute('id'))
                            elif endMonth - startMonth >= 1:
                                monthList.append( li.get_attribute('id'))
                            else:
                                self.logger.logger.info(searchKey+li.get_attribute('id')+str(startYear)+str(startMonth))
                        else:
                            self.courts.append(li.find_element_by_css_selector('a').get_attribute('href'))    

            if len(yearList)>0:
                for courtCode in yearList:
                    print(courtCode)
                    if startYear ==0:
                        self.SearchYear(searchKey,courtCode)
                    else:
                        self.SearchYear(searchKey,courtCode,startYear,endYear)
            
            if len(monthList)>0:
                for courtCode in monthList:
                    if startMonth ==0:
                        self.SearchMonth(searchKey,courtCode,startYear)
                    else:
                        self.SearchMonth(searchKey,courtCode,startYear, startMonth, endMonth)
            
            if startYear == 0 :
                self.logger.logger.info('totalNo:'+str(self.totalCount))      

        except  Exception as e: 
            self.logger.logger.error(e)
            raise e
        
    def processIter(self, searchKeys, referenceKeys, requestId):
        processCount = 0

        # raise('failed')
        for c in self.courts:
            time.sleep(0.5) 
            documents = []
            self.driver.get(c)
        
            docList=self.driver.find_elements_by_css_selector('#table3 a')
            
            for doc in docList:
                currentUrl=doc.get_attribute('href')
                content = requests.get(currentUrl, headers = self.headers)
                documents.append(self.PageAnalysis(currentUrl, content, searchKeys, referenceKeys))
                processCount += 1
                if self.totalCount >10 and processCount%(int(self.totalCount/10))==0 :
                    self.logger.logger.info(str(processCount)+'_'+str(processCount*100/self.totalCount)+'%')
                time.sleep(0.5)
                

            nextPage = self.driver.find_element_by_css_selector('#form1 > div:nth-child(3) > table:nth-child(2) > tbody > tr > td:nth-child(2) > a:nth-child(3)')
            # print(nextPage.text)

            while nextPage.is_displayed():
                time.sleep(0.5)
                nextPage.click()
                nextPage = self.driver.find_element_by_css_selector('#form1 > div:nth-child(3) > table:nth-child(2) > tbody > tr > td:nth-child(2) > a:nth-child(3)')
                
                docList= self.driver.find_elements_by_css_selector('#table3 a')
                
                for doc in docList:
                    currentUrl=doc.get_attribute('href')
                    content = requests.get(currentUrl,headers = self.headers)
                    documents.append(self.PageAnalysis(currentUrl, content, searchKeys, referenceKeys))
                    processCount += 1
                    if self.totalCount >10 and processCount%(int(self.totalCount/10))==0 :
                        self.logger.logger.info(str(processCount)+'_'+str(processCount*100/self.totalCount)+'%')
                    time.sleep(0.5)
            # self.driver.find_elements_by_css_selector('#table3 a')[0].click()
            # documents.append(self.PageAnalysis(searchKeys, referenceKeys))
            # nextPage = self.driver.find_element_by_css_selector('tbody > tr:nth-child(1) > td:nth-child(2) > a:nth-child(3)')
            

            # while nextPage.is_displayed():
            #     time.sleep(0.1)
            #     nextPage.click()
            #     documents.append(self.PageAnalysis(searchKeys, referenceKeys))
            #     nextPage = self.driver.find_element_by_css_selector('tbody > tr:nth-child(1) > td:nth-child(2) > a:nth-child(3)')
            #     processCount += 1
                
            #     if self.totalCount >10 and processCount%int(self.totalCount/10)==0:
            #         self.logger.logger.info(str(processCount)+'_'+str(processCount*100/self.totalCount)+'%')

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
                    searchKeys =  list(map(lambda x : x['key'], request['searchKeys']))
                    _id = request['_id']

                    pageSize = 10
                    totalCount = self.dataAccess.get_documents_count(str(requestId))
                    totalPages = math.ceil(totalCount/pageSize)
                    # print(str(totalPages))
                    for i in range(1,totalPages+1):
                        # print(i)
                        documents = self.dataAccess.get_allPaged_documents(str(requestId),pageSize,i)

                        for doc in documents:
                            self.dataAccess.update_document_reference(str(requestId),doc['_id'],self.ContentAnalysis(doc['content'], referenceKeys))
                            # self.dataAccess.update_document_searchKeys(str(requestId),doc['_id'],self.ContentAnalysis(doc['content'], searchKeys))
                            
                    self.dataAccess.finish_requests(_id)
                    self.logger.logger.info('finish_requests:'+requestId)
                except Exception as e:
                    self.logger.logger.error(e)

    def processNewRequest(self):
        # process new requests
        request = self.dataAccess.get_created_request()

        while request :
            try:
                requestId = request['requestId']
                self.logger.logger.info('processNewRequest:'+requestId)
                searchKeys = list(map(lambda x : x['key'], request['searchKeys']))
                referenceKeys = request['referenceKeys']
                _id = request['_id']
                self.dataAccess.processing_request(_id)

                for searchKey in searchKeys:
                    # print(searchKey)
                    time.sleep(0.5)
                    if self.Search(searchKey):
                        # print('get data')
                        self.getCourts(searchKey)
                        self.dataAccess.processing_requests(_id,searchKey,self.totalCount)
                        self.processIter(searchKeys,referenceKeys,requestId)
                    else:
                        raise Exception()

                self.dataAccess.finish_requests(_id)
                self.logger.logger.info('finish_requests:'+requestId)
            except Exception as e:
                self.logger.logger.error(e)
                self.dataAccess.failed_request(_id)
            # print(request)

            request = self.dataAccess.get_created_request()


    def processFaliedKey(self):
        # process new requests
        request = self.dataAccess.get_failed_request()

        while request :
            try:
                requestId = request['requestId']
                self.logger.logger.info('processProcessingKey:'+requestId)
                searchKeys = list(map(lambda x : x['key'], request['searchKeys']))
                referenceKeys = request['referenceKeys']
                _id = request['_id']
                self.dataAccess.processing_request(_id)

                self.dataAccess.remove_all_documents(requestId)

                for searchKey in searchKeys:
                    # print(searchKey)
                    if self.Search(searchKey):
                        # print('get data')
                        self.getCourts(searchKey)
                        self.dataAccess.processing_requests(_id,searchKey,self.totalCount)
                        self.processIter(searchKeys,referenceKeys,requestId)
                    else:
                        raise Exception()

                self.dataAccess.finish_requests(_id)
                self.logger.logger.info('finish_requests:'+requestId)
            except Exception as e:
                self.logger.logger.error(e)
                self.dataAccess.failed_request(_id)
            # print(request)

            request = self.dataAccess.get_failed_request()
    
    def process(self):
        self.logger.logger.info('Start Process')
        NEW = False
        FAIL = False
        MOD = False
        if str(sys.argv).find('NEW') > -1:
            NEW = True
        if str(sys.argv).find('FAIL') > -1:
            FAIL = True
        if str(sys.argv).find('MOD') > -1:
            MOD = True
        
        if(NEW or FAIL or MOD):
            while( not os.path.exists(self.dataPath+'process.stop')):
                try:
                    if MOD:
                        # print('MOD')
                        self.processModifiedKey()
                    if NEW:
                        # print('NEW')
                        self.processNewRequest()
                    if FAIL:
                        # print('FAIL')
                        self.processFaliedKey()
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
