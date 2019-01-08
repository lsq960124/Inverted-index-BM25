# -*- coding: utf-8 -*-
"""
Created on Mon Jan  7 16:30:53 2019

@author: 003723
"""


import jieba
import configparser
import pandas as pd
import sqlite3
jieba.load_userdict('./userdict/2000000-dict.txt')



class IndexModule:

    postings_lists = {}

    def __init__(self, config_path, config_encoding):
        self.config_path = config_path
        self.config_encoding = config_encoding
        config = configparser.ConfigParser()
        config.read(config_path, config_encoding)
        f = open(config['DEFAULT']['stop_words_path'], encoding = config['DEFAULT']['stop_words_encoding'])
        words = f.read()
        self.stop_words = set(words.split('\n'))
    

    def is_number(self, s):
        try:
            float(s)
            return True
        except ValueError:
            return False
        
    def clean_list(self, seg_list):
        cleaned_dict = {}
        n = 0
        for i in seg_list:
            i = i.strip().lower()
            if i != '' and not self.is_number(i) and i not in self.stop_words:
                n = n + 1
                if i in cleaned_dict:
                    cleaned_dict[i] = cleaned_dict[i] + 1
                else:
                    cleaned_dict[i] = 1
        return n, cleaned_dict
    
    
    def write_postings_and_knowledge_to_db(self, db_path):
        conn = sqlite3.connect(db_path)
        c = conn.cursor()
        
        c.execute('''DROP TABLE IF EXISTS postings''')
        c.execute('''CREATE TABLE postings
                     (term TEXT PRIMARY KEY, df INTEGER, docs TEXT)''')

        c.execute('''DROP TABLE IF EXISTS knowledge''')
        c.execute('''CREATE TABLE knowledge
                     (id INTEGER PRIMARY KEY, question TEXT, answer TEXT)''')

        for key, value in self.postings_lists.items():
            doc_list = '\n'.join(map(str,value[1]))
            t = (key, value[0], doc_list)
            c.execute("INSERT INTO postings VALUES (?, ?, ?)", t)
        
        conn.commit()

        for i,question in self.files.items():
            answer ='标准问“'+ question +'”的答案'
            t = (i, question, answer)
            c.execute("INSERT INTO knowledge VALUES (?, ?, ?)", t)
        
        conn.commit()
        conn.close()
    
     
    def construct_postings_lists(self):
        config = configparser.ConfigParser()
        config.read(self.config_path, self.config_encoding)
        Data = pd.read_csv(r'data\data.csv', sep='\t', header=None)
        files = set(Data[1])
        self.files = {k:v for k,v in enumerate(files)}
        
        AVG_L = 0

        for i,x in self.files.items():
            
            seg_list = jieba.lcut(x, cut_all=False)
            ld, cleaned_dict = self.clean_list(seg_list)
            AVG_L = AVG_L + ld
            
            for key, value in cleaned_dict.items():
             
                d = [i, value, ld] 
                if key in self.postings_lists:
                    self.postings_lists[key][0] = self.postings_lists[key][0] + 1 # df++
                    self.postings_lists[key][1].append(d)
                else:
                    self.postings_lists[key] = [1, [d]]# [df, [Doc]]
        AVG_L = AVG_L / len(files)
        config.set('DEFAULT', 'N', str(len(files)))
        config.set('DEFAULT', 'avg_l', str(AVG_L))
        with open(self.config_path, 'w', encoding = self.config_encoding) as configfile:
            config.write(configfile)
        self.write_postings_to_db(config['DEFAULT']['db_path'])


if __name__ == "__main__":
    im = IndexModule('./config.ini', 'utf-8')
    im.construct_postings_lists()
