from nltk.stem.porter import *
from nltk.tokenize import word_tokenize, sent_tokenize
from nltk.corpus import stopwords 
from nltk.tokenize import RegexpTokenizer
from itertools import chain

from collections import defaultdict    
from collections import OrderedDict
import os
import ast
from math import ceil
idRange_mappingFile_map = {}

def read_mapping(filepath):
    global idRange_mappingFile_map
    with open(filepath, 'r') as f:
        s = f.read()
        idRange_mappingFile_map = ast.literal_eval(s)

def read_index(path):
    index = {}
    with open(path, 'r') as f:
        s = f.read()
        index = ast.literal_eval(s)
    return index

# Assumes either pure field queries , with fields of only one term; or pure non field query; 
#NOT A MIXTURE OF FIELD AND NON FIELD
class Query_PreProcess():
    def __init__(self,q, path_to_index_folder):
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
        self.path_to_index_folder = path_to_index_folder
        
        

    
    def check_if_field_query(self):
        
        space_split = self.q.split()            
        space_split = list(map(lambda x:x.lower(),space_split))
        
        while space_split:
            terms = space_split[0]
            #if no colon
            colon_split = terms.split(':') 
            if colon_split[0] in self.fields:
                #{t:new, c:old}
                self.fquery_dict[self.fields[colon_split[0]]] = colon_split[1] 
                space_split.remove(terms)
            else:
                break
                # query like 4:50 ; not a field query, do nothing and break
        filtered_sentence = [w for w in space_split if not w in self.stop_words]
        stemmed_list = [self.ps(word) for word in filtered_sentence]
        self.nquery = stemmed_list
    
    # Returns index of x if it is present 
    # in arr[], else return -1 
    def binarySearch(self, L,target): 
        start = 0
        end = len(L) - 1

        while start <= end:
            middle = ceil((start + end)/2)
            midpoint = L[middle]
            if midpoint > target:
                end = middle - 1
            elif midpoint < target:
                start = middle + 1
            else:
                return middle
        return -1


    def parse_value_string(self, str, field):
          # value: 29$i:1#n:1#|61$i:1#n:1#|149$i:6#n:6#|197$i:1#n:1#
        val_dict = {}
        check = field+':'
        pipe_tokens = str.split('|')
        for one_token in pipe_tokens:
            if one_token != '\n':
                docId,rest = one_token.split('$')
                # split the rest on basis of field
                # rest i:1#n:1#
                if check in rest:
                    freq = rest.split(check)[1][:-1]
                    val_dict[docId] = freq
        return val_dict

#----------------------------RETREIVING PAGES---------------------------
    def retrieve_pages(self):
        # for each query term, list_of_list has a list, in which each element is a tuple. 
        # first index doc id, second frequency in that doc
        # say, query is 'hyd patna'
        # list_of_list = [[(9, 9), (10, 2)...], [(3,2), (15, 4)...]]

        # commons for both field and normal query
        list_of_list = []
        add = list_of_list.append
        common_docs = set()
        ins = common_docs.add
        phi = True
        path_wordMap = os.path.join(self.path_to_index_folder, 'WordMapping.txt')
        wordRange_index_map = read_index(path_wordMap)

        # >> ---------------   FOR NORMAL QUERY ------------------- <<
        if not self.fquery_dict:
            for term in self.nquery:
                self.results[term] = {}
                # wordRange_index_map : {1: (abc, hej), 2:(hek, nol), 3:(nom, zzw)}
                # check where does the term lie
                file_num = -1
                for index_num, tuple_range in wordRange_index_map.items():
                    if term >= tuple_range[0] and term <= tuple_range[1]:
                        #retrive this file
                        file_num = index_num
                        break
                    else:
                        pass
                        #term not present in dict
                index_to_search = 'index'+str(file_num)+'.txt'
                path_to_search = os.path.join(self.path_to_index_folder,index_to_search)
                
                terms_list = []
                vals_list = []
    
                with open(path_to_search, 'r') as i:
                    for line in i.readlines():
                        terms_list.append(line.split('|', 1)[0])
                        vals_list.append(line.split('|', 1)[1])
                
                idx = self.binarySearch(terms_list, term)
                if idx != -1:
                    value = vals_list[idx] 
                    # value: 29$i:1#n:1#|61$i:1#n:1#|149$i:6#n:6#|197$i:1#n:1#

                    terms_list = []
                    vals_list = []
                    val_dict = self.parse_value_string(value, 'n')
                    # val_dict: {29:1, 61:1, 149:6, 197:1...}
                    # add to list_of_list
                    # [(149,6), (61:1), (29,1)] this is to be added
                    add(sorted(val_dict.items(), key=lambda item: item[1], reverse=True)[:10])
        # >> ---------------   END FOR NORMAL QUERY ------------------- <<
        # >> ---------------   FOR FIELD QUERY ------------------- <<
        else: 
            occurence = []
            add_o = occurence.append
            for field, keyword in self.fquery_dict.items():
                 # single term after colon; like {t:gandhi} NOT {t:Mahatma Gandhi}
                if len(keyword.split())==1:
                    self.results[keyword] = {}
                    # find where does this keyword lie
                    file_num = -1
                    for index_num, tuple_range in wordRange_index_map.items():
                        if keyword >= tuple_range[0] and keyword <= tuple_range[1]:
                            #retrive this file
                            file_num = index_num
                            break
                        else:
                            pass
                            #term not present in dict
                    index_to_search = 'index'+str(file_num)+'.txt'
                    path_to_search = os.path.join(self.path_to_index_folder,index_to_search)
                    
                    terms_list = []
                    vals_list = []
        
                    with open(path_to_search, 'r') as i:
                        for line in i.readlines():
                            terms_list.append(line.split('|', 1)[0])
                            vals_list.append(line.split('|', 1)[1])
                    
                    idx = self.binarySearch(terms_list, keyword)

                    if idx != -1:
                        value = vals_list[idx] 
                        # value: 29$i:1#n:1#|61$i:1#n:1#|149$i:6#n:6#|197$i:1#n:1#
                        terms_list = []
                        vals_list = []
                        val_dict = self.parse_value_string(value, field)
                        # val_dict: {29:1, 61:1, 149:6, 197:1...}
                        # eg [(149,6), (61:1), (29,1)] this is to be added in list_of_list
                        add(sorted(val_dict.items(), key=lambda item: item[1], reverse=True)[:10])
                        add_o([*val_dict])
            #  ( DONE PROCESSING FOR "ONE" FIELD TERM )        
            # remove empty lists from list_of_list
            list_of_list = list(filter(None, list_of_list))
            occurence = list(filter(None, occurence))
            
            common_docs = set(occurence[0]).intersection(*occurence)
            
            if common_docs:
                phi = False
                # no need to do anything
        # >> ---------------  END FOR FIELD QUERY ------------------- <<    
        # True in case of non field query
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
        print(self.outputs)

        return self.outputs

    def find_titles(self, id_no):
        # check where does the id lie
        file_num = -1
        for index_num, tuple_range in idRange_mappingFile_map.items():
            if int(id_no) >= tuple_range[0] and int(id_no) <= tuple_range[1]:
                #retrive this file
                file_num = index_num
                break
            else:
                #term not present in dict
                pass
        if file_num!=-1:
            
            mapping_to_search = 'mapping'+str(file_num)+'.txt'
            path_to_search = os.path.join(self.path_to_index_folder,mapping_to_search)
            docs_list = []
            titles_list = []
            with open(path_to_search, 'r') as i:
                for line in i.readlines():
                    docs_list.append(line.split('|', 1)[0] )
                    titles_list.append(line.split('|', 1)[1] )
            idx = self.binarySearch(docs_list, id_no)
            if idx != -1:
                title = titles_list[idx] 
                
                docs_list = []
                titles_list = []
                        
                self.outputs.append(title)
