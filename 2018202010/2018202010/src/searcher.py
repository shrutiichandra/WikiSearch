#!/usr/bin/env python
# coding: utf-8

# In[6]:


from nltk.stem.porter import *
from nltk.tokenize import word_tokenize, sent_tokenize
from nltk.corpus import stopwords 
from nltk.tokenize import RegexpTokenizer
from itertools import chain


# In[5]:


from collections import defaultdict    
from collections import OrderedDict


# In[4]:


import ast
doc_map = {}
with open('../res/mapping.txt', 'r') as f:
    s = f.read()
    doc_map = ast.literal_eval(s)


# In[7]:


class Query_PreProcess():
    def __init__(self,q, index):
        self.q = q
        self.mine = ['br','\'','http','url','web','www','blp','ref','external','links']
        self.stop_words = set(stopwords.words('english')).union(self.mine)
        self.ps = PorterStemmer().stem
        self.tokenizer = RegexpTokenizer(r'\w+')
        self.fields = {'title':'t', 'body':'b', 'category':'c', 'infobox':'i', 'ref':'r'}
        self.fquery_dict = {}
        self.nquery = []
        self.index = index
        self.results = {}
        self.outputs = []
    def check_if_field_query(self):
        
        space_split = self.q.split()
        
#                 print('infobox: ',filtered_sentence)
        space_split = list(map(lambda x:x.lower(),space_split))
                
        for terms in space_split:
            colon_split = terms.split(':')
            if colon_split[0] in self.fields:
                self.fquery_dict[self.fields[colon_split[0]]] = colon_split[1]
                space_split.remove(terms)
            else:
                pass
                # query like 4:50 ; not a field query
        filtered_sentence = [w for w in space_split if not w in self.stop_words]
        stemmed_list = [self.ps(word) for word in filtered_sentence]
        self.nquery = stemmed_list
#         print(self.nquery)
    
    def retrieve_pages(self):
        list_of_list = []
        add = list_of_list.append
        if not self.fquery_dict:
            for term in self.nquery:
                self.results[term] = {}
                dict_of_docs = index[term] # { docid: {'t':2, 'n':3}, docid2: {'c':3,'n':3} } 
    #             print(type(dict_of_docs))
                for k,v in dict_of_docs.items():
    #                 print('here')
                    this_doc_dict = dict_of_docs[k] #{'t':2, 'n':3}
    #                 print(this_doc_dict)
                    self.results[term][k] = this_doc_dict['n']
                add(sorted(self.results[term].items(), key=lambda item: item[1], reverse=True)[:10])
            all_tuples = list(chain(*list_of_list))

            d = OrderedDict()

            for a, *b in all_tuples:
                if a in d:
                     d[a] = d[a] + b[0]
                else:
                     d[a] = b[0]
            common_docs = set()
            ins = common_docs.add
            found_top_ten = False
            for k,v in d.items():
                if len(common_docs) >= 10:
                    found_top_ten = True
                    break
                ins(k)
            
            if not found_top_ten:
                pass
                # TO DO

            print(common_docs)
            for i in common_docs:
                self.find_titles(i)
        else: # if field query
            #TO DO
            pass

    def search(self):
        self.check_if_field_query()
        self.retrieve_pages()
        return self.outputs

    def find_titles(self, id_no):
        self.outputs.append(doc_map[id_no]['title'])


# In[1]:


# query = "napier"
# q = Query_PreProcess(query, index)
# q.search()
# # print(q.nquery)
# # print(q.fquery_dict)
# # print(type(q))

