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
            "news1":"news_analyzer",
            "news2":"standard"
        }
        self.index_name = 'test'
        self.type_name = 'news'
        self._base_url = base_url
        self.base_url = base_url + self.index_name + '/' + self.type_name + '/'
        print(self.base_url)
        # set your own file_folder
        self.file_folder = 'N:\json'
        self.debug = True

    def response(self,message):
        print(message)
        return

    # first create index, do not create index and type at once
    def create_index(self):
        index_name = self.index_name
        type_name = self.type_name
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
                                "type":"ik_smart",
                                "stopwords_path":get_stopwords()
                            }
                        }
                    }
                },
                type_name: {
                    "properties": {
                        #news1 ik analyzer
                        #news2 standard analyzer
                        "title": {
                            "type": "text",
                            "analyzer": self.analyzer['news1'],
                            "search_analyzer": self.analyzer['news1'],
                        },
                        "title1":{
                            "type": "text",
                            "analyzer": self.analyzer['news2'],
                            "search_analyzer": self.analyzer['news2']
                        },
                        "text": {
                            "type": "text",
                            "analyzer": self.analyzer['news1'],
                            "search_analyzer": self.analyzer['news1'],
                        },
                        "text1": {
                            "type": "text",
                            "analyzer": self.analyzer['news2'],
                            "search_analyzer": self.analyzer['news2']
                        },
                        "url": {
                            "type": "text"
                        },
                        "comment": {
                            "type": "text",
                            "analyzer": self.analyzer['news1'],
                            "search_analyzer": self.analyzer['news1']
                        }
                    }
                }

            }
        settings_json = json.dumps(settings)
        r = requests.put(index_url, data = settings_json, headers = self.headers['json'])
        self.response(r.text)

    #create new mapping using index/type/_mapping api to create mapping
    def create_mapping(self, mappings = {}):
        mapping_url = self.base_url + '_mapping'
        # http://www.jianshu.com/p/f169e70364d4
        if mappings:
            mapping_request = mappings
        else:
            return
        r = requests.put(mapping_url, data = json.dumps(mapping_request), headers = self.headers['json'])
        self.response(r.text)

    #不默认指明index，以免误删除
    def delete_index(self):
        index_url = self._base_url + self.index_name
        r = requests.delete(index_url, headers = self.headers['normal'])
        self.response(r.text)

    #batch delete
    def delete_data(self,  query={}):
        delete_url = self.base_url + '_delete_by_query?pretty=true'
        #if query is empty, delete all data
        if not query:
            query = {
                "query":{
                    "match_all":{

                    }
                }
            }
            query_request = json.dumps(query, ensure_ascii='False')
        else:
            query_request = json.dumps(query, ensure_ascii='False')
        r = requests.post(delete_url, data = query_request, headers = self.headers['json'])
        self.response(r.text)

    def insert_data(self, num):
        file_folder = self.file_folder
        bulk_url = self.base_url + '_bulk'
        # get bulk data -> ready to bulk insert
        for bulk_data in self.generate_bulk_insert_data(file_folder, num):
            self.response(bulk_data)
            r = requests.post(bulk_url, data = bulk_data.encode('utf-8'), headers = self.headers['json'])
            self.response(r.content)
            # 若为调试，只插入一批次
            assert not self.debug

    def generate_bulk_insert_data(self, file_folder, num):
        read_file = ReadFile.ReadFile(file_folder)
        # return list data -> generate bulk data
        # list data contains json like data
        # add action and meta data to every json data
        # 进行数据冗余
        # 从url指定位置，减少每次action动作的数据
        action = { "index":  {}}
        bulk_data = ""
        for data_list in read_file.get_data(num):
            # data list -> data
            # 传过来的data为从源文件取出的json格式
            # 对content做数据冗余，先将json载入为map，再添加，然后再dumps为json格式
            for data in data_list:
                data_map = json.loads(data,encoding='utf-8')
                data_map['title1'] = data_map['title']
                data_map['text1'] = data_map['text']
                bulk_data += json.dumps(action, ensure_ascii=False) + '\n' + json.dumps(data_map, ensure_ascii=False) + '\n'
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

    # generate query json and call matched search function
    # 1. query need to be json format
    # 2. query need to be encode 'utf-8' when requests.get
    def input_search(self, query):
        if '?' in query or '*' in query:
            query_json = {
                "query":{
                    "query_string":{
                        "fields":['text1','text','title','title1'],
                        "query": query,
                        "analyze_wildcard": "true",
                        'allow_leading_wildcard': "true"
                    }
                }
            }
            self.wildcard_search(query = json.dumps(query_json, ensure_ascii=False).encode('utf-8'))
        else:
            query_json = {
                "query":{
                    "match":{
                        "text":query
                    }
                }
            }
            self.standard_search(query = json.dumps(query_json, ensure_ascii=False).encode('utf-8'))

    def wildcard_search(self,  query = None):
        search_url = self.base_url + '_search?pretty=true'
        r = requests.get(search_url, data = query, headers = self.headers['json'])
        self.response(r.text)

    def standard_search(self,  query = None):
        search_url = self.base_url +'_search?pretty=true'
        if query:
            r = requests.get(search_url, data = query, headers = self.headers['json'])
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
        analyzed_url = self._base_url + '_analyze'

        analyze_request_raw = {
            "analyzer": 'chinese',
            "text": "中国科学院大学是世界上最好的大学"
        }
        analyze_request_data = json.dumps(analyze_request_raw, ensure_ascii=False)
        r = requests.get(analyzed_url, data = analyze_request_data.encode('utf-8'), headers = self.headers['json'])
        print(r.text)

    def restart(self):
        es.delete_index()


es = ESClass("http://localhost:9200/")
#es.create_index()
es.input_search(query='*奥会')
#es.analyzed_test()
#es.insert_data('news_test','news','N:\json',200)
#es.input_search(query = "5800X*Music")
#es.insert_data(num=1)
es.show_index()
#es.show_type()