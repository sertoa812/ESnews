
def get_stopwords():
    with open ('stopwords.txt', 'rt', encoding='utf-8') as f:
        return [line.replace('\n','') for line in f]