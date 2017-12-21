import json
import requests
import ReadFile
from GetStopWords import get_stopwords
class ESClass(object):
    def __init__(self, base_url):
        self.headers = {
            'json':{'Content-Type':'application/json'},
            'normal':{}
        }
        self.analyzer = {
            "ik_max":"ik_max_word",
            "ik_smart":"ik_smart",
            "news":"news_analyzer",
        }
        self.index_name = 'test'
        self.type_name = 'news'
        self._base_url = base_url
        self.base_url = base_url + self.index_name + '/' + self.type_name + '/'
        print(self.base_url)
        # set your own file_folder
        self.file_folder = ''

    def response(self,message):
        print(message)
        return

    # first create index, do not create index and type at once
    def create_index(self, index_name = None, type_name = None):
        if not index_name: index_name = self.index_name
        if not type_name: type_name = self.type_name
        index_url = self._base_url + index_name
        r = requests.get(index_url)
        #index already exists
        if r.status_code != 404:
            self.response("Index \""+ index_name + "\" already exists")
            return
        settings = \
            {
                "settings":{
                    "number_of_shards" :   1,
                    "number_of_replicas" : 0,
                    "analysis":{
                        "analyzer":{
                            "news_analyzer":{
                                "type":"ik_max_word",
                                "stopwords_path":get_stopwords()
                            }
                        }
                    }
                },
                type_name: {
                    "properties": {
                        "title": {
                            "type": "text",
                            "analyzer": self.analyzer['news'],
                            "search_analyzer": self.analyzer['news'],
                        },
                        "text": {
                            "type": "text",
                            "analyzer": self.analyzer['news'],
                            "search_analyzer": self.analyzer['news'],
                        },
                        "url": {
                            "type": "text"
                        },
                        "comment": {
                            "type": "text"
                        }
                    }
                }

            }
        settings_json = json.dumps(settings)
        r = requests.put(index_url, data = settings_json, headers = self.headers['json'])
        self.response(r.text)

    #create new mapping using index/type/_mapping api to create mapping
    def create_mapping(self, index_name = None, type_name = None, mappings = {}):
        mapping_url = self.base_url + '_mapping'
        # http://www.jianshu.com/p/f169e70364d4
        if mappings:
            mapping_request = mappings
        else:
            return
        r = requests.put(mapping_url, data = json.dumps(mapping_request), headers = self.headers['json'])
        self.response(r.text)

    def delete_index(self, index_name):
        index_url = self._base_url + index_name
        r = requests.delete(index_url, headers = self.headers['normal'])
        self.response(r.text)

    #batch delete
    def delete_data(self, index_name = None, type_name = None, query={}):
        delete_url = self.base_url + '/_delete_by_query?pretty=true'
        #if query is empty, delete all data
        if query:
            query = {
                "query":{
                    "match":{

                    }
                }
            }
            query_request = json.dumps(query, ensure_ascii='False')
        else:
            query_request = json.dumps(query, ensure_ascii='False')
        r = requests.post(delete_url, data = query_request, headers = self.headers['json'])
        self.response(r.text)

    def insert_data(self, num):
        index_name = self.index_name
        type_name = self.type_name
        file_folder = self.file_folder
        bulk_url = self._base_url + '_bulk'
        # get bulk data -> ready to bulk insert
        for bulk_data in self.generate_bulk_insert_data(index_name, type_name, file_folder, num):
            self.response(bulk_data)
            r = requests.post(bulk_url, data = bulk_data.encode('utf-8'), headers = self.headers['json'])
            self.response(r.content)

    def generate_bulk_insert_data(self, index_name, type_name, file_folder, num):
        read_file = ReadFile.ReadFile(file_folder)
        # return list data -> generate bulk data
        # list data contains json like data
        # add action and meta data to every json data
        action = { "index":  { "_index": index_name, "_type": type_name}}
        bulk_data = ""
        for data_list in read_file.get_data(num):
            # data list -> data
            for data in data_list:
                bulk_data += json.dumps(action, ensure_ascii=False) + '\n' + data + '\n'
            bulk_data += '\n'
            yield(bulk_data)
            bulk_data = ""

    def show_index(self):
        cat_url = self._base_url + '_cat/indices?v'
        r = requests.get(cat_url)
        self.response(r.text)

    def show_type(self):
        type_url = self._base_url + self.index_name + '/_search?pretty=true'
        r = requests.get(type_url)
        self.response(r.text)

    def search(self, index_name = None, type_name = None, query = {}):
        search_url = self.base_url +'_search?pretty=true'
        if query:
            query_request = json.dumps(query, ensure_ascii='False')
            r = requests.get(search_url, data = query_request, headers = self.headers['json'])
        else:
            r = requests.get(search_url, headers = self.headers['normal'])
        self.response(r.text)
    #test function
    def search_test(self, query = {}):
        search_url = self.base_url + '_search'
        #Decide which header depend on query
        if query:
            headers = self.headers['normal']
        else:
            headers = self.headers['json']
        r = requests.get(search_url, data = query, headers = headers)
        self.response(r.text)
    #test ik_analyzer token result by inputing a constant text
    def analyzed_test(self):
        analyzed_url = self.base_url + '_analyze'

        analyze_request_raw = {
            "analyzer": self.analyzer['smart'],
            "text": "中国科学院大学是世界上最好的大学"
        }
        analyze_request_data = json.dumps(analyze_request_raw, ensure_ascii=False)
        r = requests.get(analyzed_url, data = analyze_request_data.encode('utf-8'), headers = self.headers['json'])
        print(r.text)

    def restart(self):
        es.delete_data()


es = ESClass("http://localhost:9200/")
#es.create_index()
#es.insert_data('news_test','news','N:\json',200)
query = {
  "query" : { "match" : { "text" : "新闻" }}
}
es.search(query = query)
#es.insert_data(num=1)
es.show_index()
#es.show_type()