#bash index.sh <path_to_dump> <path_to_index_folder>
#path_to_dump: /home/shruti/Documents/pg2k18/sem3/ire/mini_proj/phase2/dataset/enwiki-latest-pages-articles.xml
#path_to_index: /home/shruti/Documents/pg2k18/sem3/ire/mini_proj/phase1/index_folder
import json
from sys import argv,exit
import os
import xml.sax as sx
import time
import pandas as pd
import text_processing as tp
import heap_ops as hp
class WikiXmlHandler(sx.handler.ContentHandler):
    
    def __init__(self,path_to_index_folder):
        sx.handler.ContentHandler.__init__(self)
        self._buffer = None
        self._values = {}
        self._current_tag = None
        self._pageNumber = 0
        self._blockNum = 0
        self._doc_map = {}
        self.another_map = {}
        self.chunk = 0
        self.check = False
        self.map_id_idx = {}
        self.path_to_index_folder = path_to_index_folder
        
    def characters(self, content): # when a character is read
        if self._current_tag:
            self._buffer.append(content)

    def startElement(self, name, attrs): # when a tag opens
        if name == 'page':
            self._pageNumber += 1
            # print(self._pageNumber,end=',')
            self._blockNum += 1
        if name in ('title','text'):
            self._current_tag = name
            self._buffer = []

    def endElement(self, name): # when a tag closes
        if name == self._current_tag:
            self._values[name] = ' '.join(self._buffer)        

        if name == 'page': # when page ends
            # now process the text
            if self._blockNum == 10000:

                self.chunk += 1
                self.check = True
                print('done chunk ', self.chunk)
                #stop parsing now
                #make the index 
                print('len map: ',len(self.another_map))
                Text_Preprocessing = tp.Text_Preprocessing(self.another_map)

                Text_Preprocessing.make_index()                
                path2index = os.path.join(self.path_to_index_folder, 'temp'+str(self.chunk-1)+'.txt')
                Text_Preprocessing.parse_posting_list(path2index)
                print('done temp',str(self.chunk-1),'.txt')
                
                path2docMap = os.path.join(self.path_to_index_folder, 'mapping'+str(self.chunk-1)+'.txt')
                i = 0
                rng = tuple()
                with open(path2docMap, 'w') as f:
                
                    for doc_id, title in self._doc_map.items():
                        if i==0:
                            rng += (doc_id,)
                        i+=1
                        if i == len(self._doc_map):
                            rng += (doc_id,)    
                        line = str(doc_id) + "|" + title + '\n'
                        f.write(line)

                self.map_id_idx[self.chunk-1] = rng
                

            if self.check:
            #renew the counts
                self._blockNum = 0
                self._doc_map = {}
                self.another_map = {}
                self.check = False
            
            self._doc_map[self._pageNumber]  = self._values['title']
            self.another_map[self._pageNumber]  = ({'id':self._pageNumber, 'title': self._values['title'],'body':self._values['text']})



path2dump = argv[1]
path_to_index_folder = argv[2]


def  parse_xml_and_index(path_to_index_folder, path2dump):
    
    parser = sx.make_parser()
    parser.setFeature(sx.handler.feature_namespaces, 0)
    handler = WikiXmlHandler(path_to_index_folder)
    parser.setContentHandler(handler)
    parser.parse(path2dump)

    if handler.another_map:
        handler.chunk += 1
        handler.check = True
        print('done chunk ', handler.chunk)
        # stop parsing now
        # make the index 
        print('len map: ',len(handler.another_map))
        Text_Preprocessing = tp.Text_Preprocessing(handler.another_map)

        Text_Preprocessing.make_index()                

        path2index = os.path.join(path_to_index_folder, 'temp'+str(handler.chunk-1)+'.txt')
        Text_Preprocessing.parse_posting_list(path2index)
        path2docMap = os.path.join(path_to_index_folder, 'mapping'+str(handler.chunk-1)+'.txt')
        i = 0
        rng = tuple()
        with open(path2docMap, 'w') as f:
        
            for doc_id, title in handler._doc_map.items():
                if i==0:
                    rng += (doc_id,)
    
                i+=1
                if i == len(handler._doc_map):
                    rng += (doc_id,)

                line = str(doc_id) + "|" + title + '\n'
                f.write(line)
        handler.map_id_idx[handler.chunk-1] = rng
    
    completedocMap = os.path.join(path_to_index_folder, 'mapping.txt')
    with open(completedocMap, 'w') as f:
        print(handler.map_id_idx, file=f)
    

        

    

start_time = time.time()        
parse_xml_and_index(path_to_index_folder,path2dump)
print('temp indices and parsing : ', (time.time()-start_time)/60.0, 'min ')

#TO DO: change hardcoded 3
def  merge_index_files(num_chunks, path_to_index_folder):
    big_dict = {}
    heap = hp.MinHeap()
    file_pointers = [None] * num_chunks

    for n in range(num_chunks):
        index_ = 'temp'+str(n)+'.txt'
        path = os.path.join(path_to_index_folder,index_)
        file_pointers[n] = open(path, 'r') # 0,1,2
    
    index_ = 'temp0.txt' #temp0, temp1, temp2
    path = os.path.join(path_to_index_folder,index_)
    file_pointers[0] = open(path, 'r') # 0,1,2
    first_line = file_pointers[0].readline() #0,1,2
    key, val = first_line.split('|', 1)
    val = val.strip('\n')
    big_dict[key] = val
    
    heap.insertKey(key) # (('a', 1)), ('b',2), ('c', 3) )

    n = 1
    last = False
    
    #start by reading temp index1
    while heap.heap:
        
        index_ = 'temp'+str(n)+'.txt' #temp0, temp1, temp2
        path = os.path.join(path_to_index_folder,index_)

        first_line = file_pointers[n].readline() #0,1,2
        # print('reading', index_)
        if first_line:
            key, val = first_line.split('|', 1)
            val = val.strip('\n')

        else:
            last = True
        
        try:    
            # print('key:',key)
            #key is already present; read from this file
            x = big_dict[key]
            big_dict[key] = x + val
            # print('merged val of ',key)

        except:    
            big_dict[key] = val
            heap.insertKey(key) 
            # print('insertKey: ',key, ' now heap: ',heap.heap)
            n = (n+1)%num_chunks
    
        if len(heap.heap)==num_chunks or last:
            minimum_key = heap.extractMin() #pop
            # print('min extracted ',minimum_key, 'now heap: ',heap.heap)
                        
    print(len(big_dict))
    # close the files
    for n in range(num_chunks):
        index_ = 'temp'+str(n)+'.txt'
        path = os.path.join(path_to_index_folder,index_)
        file_pointers[n].close()
        os.remove(path)
    return big_dict

start_time = time.time()        
dict_index = merge_index_files(3, path_to_index_folder)
print("MERGING:  %s min ---" % ((time.time() - start_time)/60.0))

def parse_dict(index, path_to_index_folder,parts=4):
    index = dict(sorted(index.items()))
    map_words_dict = {}
    size_big_idx = len(index)
    
    part_size = size_big_idx//parts
    
    chunk = 0
    block = 0   
    lines_list = []
    
    for term,posting_list in index.items():
        line = term + "|" + posting_list +'\n'
        chunk += 1
        
        lines_list.append(line)
        
        if chunk == part_size:
            
            block += 1
            chunk = 0
            begin = lines_list[0].split('|', 1)[0]
            end = lines_list[-1].split('|', 1)[0]
            map_words_dict[block] = (begin,end)
            path2index = os.path.join(path_to_index_folder, 'index'+str(block)+'.txt')                            
            with open(path2index, 'w+') as i:
                for l in lines_list:
                    i.write(l)
            lines_list = [] 
            
    if lines_list:
        block += 1
        print(lines_list)
        begin = lines_list[0].split('|', 1)[0]
        end = lines_list[-1].split('|', 1)[0]
        map_words_dict[block] = (begin,end)
        path2index = os.path.join(path_to_index_folder, 'index'+str(block)+'.txt')                            
        with open(path2index, 'w+') as i:
            for l in lines_list:
                i.write(l)
            lines_list = [] 
    # print(map_words_dict)

    # delete temp indices
    completeWordMap = os.path.join(path_to_index_folder, 'WordMapping.txt')
    with open(completeWordMap, 'w') as f:
        print(map_words_dict, file=f)



parse_dict(dict_index, path_to_index_folder)