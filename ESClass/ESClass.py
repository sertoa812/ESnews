import json
import requests
import ReadFile
class ESClass(object):
    def __init__(self, base_url):
        self.base_url = base_url
        self.headers = {
            'json':{'Content-Type':'application/json'},
            'normal':{}
        }

    def response(self,message):
        print(message)
        return

    def create_index(self, index_name):
        base_url = self.base_url
        index_url = base_url + index_name
        r = requests.get(index_url)
        #index already exists
        if r.status_code != 404:
            self.response("Index \""+ index_name+ "\" already exists")
            return
        settings = \
            {
                "settings":{
                    "number_of_shards" :   1,
                    "number_of_replicas" : 0
                },
                "mappings": {
                    "news": {
                        "properties": {
                            "title": {"type": "text"},
                            "name": {"type": "text"},
                            "url": {"type": "text"},
                            "comment": {"type": "text"}
                        }
                    }
                }
            }
        settings_json = json.dumps(settings)
        r = requests.put(index_url, data = settings_json, headers = self.headers['json'])
        self.response(r.text)

    def create_mapping(self, index_name, mappings):
        mapping_url = self.base_url + index_name
        r = requests.put(mapping_url, data = mappings, headers = self.headers['json'])
        self.response(r.text)

    def delete_index(self, index_name):
        base_url = self.base_url
        index_url = base_url + index_name
        r = requests.delete(index_url, headers = self.headers['normal'])
        self.response(r.text)

    def show_index(self):
        base_url = self.base_url
        cat_url = base_url + '_cat/indices?v'
        r = requests.get(cat_url)
        self.response(r.text)

    def insert_data(self, file_folder, num):
        bulk_url = self.base_url + '_bulk'
        # get bulk data -> ready to bulk insert
        for bulk_data in self.generate_bulk_insert_data(file_folder, num):
            r = requests.post(bulk_url, data = bulk_data.encode('utf-8'), headers = self.headers['json'])
            self.response(r.content)

    def generate_bulk_insert_data(self, file_folder, num):
        read_file = ReadFile.ReadFile(file_folder)
        # return list data -> generate bulk data
        # list data contains json like data
        # add action and meta data to every json data
        action = { "index":  { "_index": "test", "_type": "news"}}
        bulk_data = ""
        for data_list in read_file.get_data(num):
            # data list -> data
            for data in data_list:
                action['index']['_source'] = data
                bulk_data += json.dumps(action, ensure_ascii=False) + '\n'
            yield(bulk_data)
            bulk_data = ""

    def search(self, query = {}):
        search_url = self.base_url + '_search'
        #Decide which header depend on query
        if query:
            headers = self.headers['normal']
        else:
            headers = self.headers['json']
        r = requests.get(search_url, data = query, headers = self.headers['normal'])
        self.response(r.text)

es = ESClass("http://localhost:9200/")
es.show_index()
es.search()