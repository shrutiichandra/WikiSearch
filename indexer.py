#bash index.sh <path_to_dump> <path_to_index_folder>
#path_to_dump: /home/shruti/Documents/pg2k18/sem3/ire/mini_proj/phase2/dataset/enwiki-latest-pages-articles.xml
#path_to_index: /home/shruti/Documents/pg2k18/sem3/ire/mini_proj/phase1/index_folder
import json
from sys import argv
import os
import xml.sax as sx
import time
import pandas as pd
import text_processing as tp

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
                
                i = Text_Preprocessing.make_index()                
                print('making index',str(self.chunk),'.txt')
                path2index = os.path.join(self.path_to_index_folder, 'index'+str(self.chunk)+'.txt')

                with open(path2index, 'w') as f:
                    f.write(json.dumps(i))
                # gc.collect()
            if self.check:
                #renew the counts
                self._blockNum = 0
                self._doc_map = {}
                self.another_map = {}
                self.check = False
            
            self._doc_map[self._pageNumber]  = ({'id':self._pageNumber, 'title': self._values['title']})
            self.another_map[self._pageNumber]  = ({'id':self._pageNumber, 'title': self._values['title'],'body':self._values['text']})



path2dump = argv[1]
path_to_index_folder = argv[2]
start_time = time.time()

parser = sx.make_parser()
parser.setFeature(sx.handler.feature_namespaces, 0)
handler = WikiXmlHandler(path_to_index_folder)
parser.setContentHandler(handler)
parser.parse(path2dump)
#for the last chunk
if handler.another_map:
    handler.chunk += 1
    handler.check = True
    print('done chunk ', handler.chunk)
    #stop parsing now
    #make the index 
    print('len map: ',len(handler.another_map))
    Text_Preprocessing = tp.Text_Preprocessing(handler.another_map)

    i = Text_Preprocessing.make_index()                

    path2index = os.path.join(path_to_index_folder, 'index'+str(handler.chunk)+'.txt')

    with open(path2index, 'w') as f:
        f.write(json.dumps(i))
print("Parsing:  %s min ---" % ((time.time() - start_time)/60.0))

def  merge_index_files(num_chunks, path_to_index_folder):

    pass
    
merge_index_files(handler.chunk, path_to_index_folder)

'''
d[1] = {'id':1, 'title':Abc, 'body': xjse}
'''

# start_time2 = time.time()
# all_map = handler.another_map
# indexer = Text_Preprocessing(all_map)
# i = indexer.make_index()



# path2index = os.path.join(path_to_index_folder, 'index.txt')

# with open(path2index, 'w') as f:
#     print(i, file=f)

# path2docMap = os.path.join(path_to_index_folder, 'mapping.txt')

# with open(path2docMap, 'w') as f:
#     print(handler._doc_map, file=f)