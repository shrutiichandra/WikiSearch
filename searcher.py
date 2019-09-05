from nltk.stem.porter import *
from nltk.tokenize import word_tokenize, sent_tokenize
from nltk.corpus import stopwords 
from nltk.tokenize import RegexpTokenizer
from itertools import chain

from collections import defaultdict    
from collections import OrderedDict

import ast
doc_map = {}

def read_mapping(filepath):
    global doc_map
    with open(filepath, 'r') as f:
        s = f.read()
        doc_map = ast.literal_eval(s)

def read_index(path):
    index = {}
    with open(path, 'r') as f:
        s = f.read()
        index = ast.literal_eval(s)
    return index

# Assumes either pure field queries , with fields of only one term; or pure non field query; 
#NOT A MIXTURE OF FIELD AND NON FIELD
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
        self.results = {}
        self.outputs = []
        
        self.index = index

    
    def check_if_field_query(self):
#         print('processing ', self.q)
        space_split = self.q.split()
        
        

        space_split = list(map(lambda x:x.lower(),space_split))
#         print(space_split)
        
        while space_split:
            terms = space_split[0]
            
            colon_split = terms.split(':') #if no colon
#             print(colon_split)
            if colon_split[0] in self.fields:
                self.fquery_dict[self.fields[colon_split[0]]] = colon_split[1] #{t:new, c:old}
                space_split.remove(terms)
            else:
                break
                # query like 4:50 ; not a field query, do nothing and break
        filtered_sentence = [w for w in space_split if not w in self.stop_words]
        stemmed_list = [self.ps(word) for word in filtered_sentence]
        self.nquery = stemmed_list
#         print(self.nquery)
    
    def retrieve_pages(self):
        # for each query term, list_of_list has a list, in which each element is a tuple. 
        #first index doc id, second frequency in that doc
        # say, query is 'hyd patna'
        # list_of_list = [[(9, 9), (10, 2)...], [(3,2), (15, 4)...]]
        list_of_list = []
        add = list_of_list.append
        common_docs = set()
        ins = common_docs.add
        phi = True
#         print('nquery: ',self.nquery)
        if not self.fquery_dict:
#             print('not a field query')
            for term in self.nquery:
#                 print('term: ',term)
                self.results[term] = {}
                dict_of_docs = self.index[term] # { docid: {'t':2, 'n':3}, docid2: {'c':3,'n':3} } 
                for k,v in dict_of_docs.items():
                    this_doc_dict = dict_of_docs[k] #{'t':2, 'n':3}
                    self.results[term][k] = this_doc_dict['n']
                add(sorted(self.results[term].items(), key=lambda item: item[1], reverse=True)[:10])
        
        else: # if field query
            occurence = []
            add_o = occurence.append
            for field, keyword in self.fquery_dict.items():
                doc_ids = []
                add_d = doc_ids.append
                if len(keyword.split())==1:
                    self.results[keyword] = {}
                    dict_of_docs = self.index[keyword] # { docid: {'t':2, 'n':3}, docid2: {'c':3,'n':3} } 
        
                    for docId,map_ in dict_of_docs.items():
                        this_doc_dict = dict_of_docs[docId] #{'t':2, 'n':3}
                        if field in this_doc_dict:
                            self.results[keyword][docId] = this_doc_dict[field]
                            add_d(docId)
                        
                    add(sorted(self.results[keyword].items(), key=lambda item: item[1], reverse=True)[:10])
                    add_o(doc_ids)
            list_of_list = list(filter(None, list_of_list))
            occurence = list(filter(None, occurence))
            
#             print(list_of_list)

            
            common_docs = set(occurence[0]).intersection(*occurence)
            
            
            if common_docs:
                phi = False
                # no need to do anything
                
        if phi:
            all_tuples = list(chain(*list_of_list))
            d = OrderedDict()

            for a, *b in all_tuples:
                if a in d:
                     d[a] = d[a] + b[0]
                else:
                     d[a] = b[0]
            
        
            found_top_ten = False
            for k,v in d.items():
                if len(common_docs) >= 10:
                    found_top_ten = True
                    break
                ins(k)

            if not found_top_ten:
                pass

        # do this for both field, non field query
        for i in common_docs:
            self.find_titles(i)
        
    def search(self):
        self.check_if_field_query()
        self.retrieve_pages()
        return self.outputs

    def find_titles(self, id_no):
        self.outputs.append(doc_map[id_no]['title'])

# index_file = '../res/index/index.txt'
# index = read_index(index_file)

# query = "arjun"
# q = Query_PreProcess(query, index)
# o = q.search()
# print(o)
# # print(q.nquery)
# # print(q.fquery_dict)
# # print(type(q))