import os
import json

class ReadFile(object):
    def __init__(self,root_dir):
        self.root_dir = root_dir

    def read_file(self, path):
        with open(path, 'rt', encoding='utf-8') as f:
            data = f.read()
        return data

    def get_data(self, num):
        data_list = []
        for index, lists in enumerate(os.listdir(self.root_dir)):
            path = os.path.join(self.root_dir, lists)
            data_list.append(self.read_file(path))
            if index % num == 0 and index != 0:
                yield(data_list)
                data_list = []

    def filter(self, data):
        return data.replace('','').replace()

