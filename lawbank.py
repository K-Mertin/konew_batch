from Crawler import Crawler
from DataAccess import DataAccess
import re, math
import time

class LawBankParser:

    def __init__(self, driver, dataAccess):
        self.driver = driver
        self.dataAccess = dataAccess
    
    def PageAnalysis(self, searchKeys, referenceKeys):
        document = {}
        # print("標題:"+driver.find_element_by_css_selector('.Table-List tr:nth-child(1)> td:nth-child(2)').text)
        title = self.driver.find_element_by_css_selector('.Table-List tr:nth-child(1)> td:nth-child(2)').text
        # print("日期:"+driver.find_element_by_css_selector('.Table-List tr:nth-child(2)> td:nth-child(2)').text)
        date = self.driver.find_element_by_css_selector('.Table-List tr:nth-child(2)> td:nth-child(2)').text
        # print("案由:"+driver.find_element_by_css_selector('.Table-List tr:nth-child(3)> td:nth-child(2)').text)
        reason = self.driver.find_element_by_css_selector('.Table-List tr:nth-child(3)> td:nth-child(2)').text
        # print("---------------------------------------------------------------------------------------------------")
        # print("內容:"+driver.find_element_by_css_selector('.Table-List tr:nth-child(5)> td:nth-child(1)').text)
        content = self.driver.find_element_by_css_selector('.Table-List tr:nth-child(5)> td:nth-child(1)').text
        # print(re.sub( '\[.*\]', '', title, count=0, flags=0))
        # print(title.split(' ')[0])
        # print(re.sub( '\[.*\]', '', title, count=0, flags=0)[-5:-1])
        # print(date)
        url = self.driver.current_url

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
    
    def getCourts(self):
        self.driver.switch_to_default_content()
        self.driver.switch_to_frame('menuFrame')
        lists = self.driver.find_elements_by_css_selector('li')

        self.courts = []
        self.totalCount = 0

        for li in lists:
            if not li.text.endswith('(0)'):
                self.totalCount += int(re.search( r'\(.*\)', li.text).group().replace('(','').replace(')',''))
                self.courts.append(li.find_element_by_css_selector('a').get_attribute('href'))
        
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

def searchKeyMap(searchKey):
    return searchKey['key']

def processModifiedKey(dataAccess, parser):
    #process modified referenceKey
    requests = dataAccess.get_modified_requests()
    
    if requests.count()>0 :
        for request in requests:
            requestId = request['requestId']
            referenceKeys = request['referenceKeys']
            _id = request['_id']

            pageSize = 10
            totalCount = dataAccess.get_documents_count(str(requestId))
            totalPages = math.ceil(totalCount/pageSize)

            for i in range(1,totalPages+1):
                documents = dataAccess.get_allPaged_documents(str(requestId),pageSize,i)
                for doc in documents:
                    dataAccess.update_document_reference(str(requestId),doc['_id'],parser.ContentAnalysis(doc['content'], referenceKeys))
                    
            dataAccess.finish_requests(_id)

def processNewRequest(dataAccess, parser):
    # process new requests
    requests = dataAccess.get_created_requests()

    if requests.count()>0 :
        
        for request in requests:
            requestId = request['requestId']
            searchKeys = list(map(lambda x : x['key'], request['searchKeys']))
            referenceKeys = request['referenceKeys']
            _id = request['_id']

            for searchKey in searchKeys:
                print(searchKey)
                parser.Search(searchKey)
                parser.getCourts()
                dataAccess.processing_requests(_id,searchKey,parser.totalCount)
                parser.processIter(searchKeys,referenceKeys,requestId)

            dataAccess.finish_requests(_id)
            
            print(request)

def processProcessingKey(dataAccess, parser):
    # process new requests
    requests = dataAccess.get_processing_requests()

    if requests.count()>0 :
        
        for request in requests:
            requestId = request['requestId']
            searchKeys = list(map(lambda x : x['key'], request['searchKeys']))
            referenceKeys = request['referenceKeys']
            _id = request['_id']

            dataAccess.remove_all_documents(requestId)

            for searchKey in searchKeys:
                print(searchKey)
                parser.Search(searchKey)
                parser.getCourts()
                dataAccess.processing_requests(_id,searchKey,parser.totalCount)
                parser.processIter(searchKeys,referenceKeys,requestId)

            dataAccess.finish_requests(_id)
            
            print(request)    

def main():
    dataAccess = DataAccess()
    crawler = Crawler()
    parser = LawBankParser(crawler.driver, dataAccess)

    processModifiedKey(dataAccess, parser)
    
    processNewRequest(dataAccess, parser)

    processProcessingKey(dataAccess, parser)

if __name__ == '__main__':
    main()
