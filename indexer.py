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
                t = time.time()
                Text_Preprocessing.make_index()                
                print('time to make index: ',time.time()-t)
                path2index = os.path.join(self.path_to_index_folder, 'temp'+str(self.chunk-1)+'.txt')
                # s=False
                # if self.chunk-1==0:
                #     s=True
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
        
    return handler.chunk
        

    

start_time = time.time()        
num_chunks = parse_xml_and_index(path_to_index_folder,path2dump)
print('temp indices and parsing : ', (time.time()-start_time)/60.0, 'min ')
# num_chunks = 3

def parse_dict(index, path_to_index_folder,map_words_dict, block):
    index = dict(sorted(index.items()))
    
       
    lines_list = []
    
    for term,posting_list in index.items():
        line = term + "|" + posting_list +'\n'
  
        
        lines_list.append(line)
        
        begin = lines_list[0].split('|', 1)[0]
        end = lines_list[-1].split('|', 1)[0]
        map_words_dict[block] = (begin,end)
        path2index = os.path.join(path_to_index_folder, 'index'+str(block)+'.txt')                            
        with open(path2index, 'w+') as i:
            for l in lines_list:
                i.write(l)

def  merge_index_files(num_chunks, path_to_index_folder):
    big_dict = {}
    map_words_dict = {}
    block = 0
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
    
    heap.insertKey((key,0)) 

    n = 1
    last = [False]*num_chunks
    t=time.time()
    #start by reading temp index1
    while heap.heap:
        key,val = None,None
    
        index_ = 'temp'+str(n)+'.txt' #temp0, temp1, temp2
        path = os.path.join(path_to_index_folder,index_)
        first_line = file_pointers[n].readline() #0,1,2
    
        if first_line:
            key, val = first_line.split('|', 1)
            
            val = val.strip('\n')
    
            try:    
                x = big_dict[key]
                big_dict[key] = x + val
            except:    
                big_dict[key] = val

            heap.insertKey((key,n)) 


            if len(big_dict)==10000:
                block += 1
                exclude_keys = [key]
                big_dict = {k: big_dict[k] for k in set(list(big_dict.keys())) - set(exclude_keys)}
                print('parsing dict for block ',block)
                t = time.time()
                parse_dict(big_dict, path_to_index_folder, map_words_dict, block)
                print('parsing done in %f sec'%time.time()-t)
                big_dict = {}
    
        else:
            last[n] = True
    

        if len(heap.heap)==num_chunks or last[n]:
    
            minimum_key = heap.extractMin() #pop
    
            n = minimum_key[1]
    
        else:
            n = (n+1)%num_chunks       
    
    if len(big_dict)>0:
        block += 1
        print('parsing dict for block ',block)
        parse_dict(big_dict, path_to_index_folder, map_words_dict, block)
        big_dict = {}
    
    print('creating WordMapping')
    completeWordMap = os.path.join(path_to_index_folder, 'WordMapping.txt')
    with open(completeWordMap, 'w') as f:
        print(map_words_dict, file=f)
    # close the files
    for n in range(num_chunks):
        index_ = 'temp'+str(n)+'.txt'
        path = os.path.join(path_to_index_folder,index_)
        file_pointers[n].close()
        os.remove(path)
    

start_time = time.time()        
merge_index_files(num_chunks, path_to_index_folder)
print("MERGING:  %s min ---" % ((time.time() - start_time)/60.0))