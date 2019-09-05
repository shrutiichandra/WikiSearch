#bash index.sh <path_to_dump> <path_to_index_folder>
#path_to_dump: /home/shruti/Documents/pg2k18/sem3/ire/mini_proj/phase2/dataset/enwiki-latest-pages-articles.xml
#path_to_index: /home/shruti/Documents/pg2k18/sem3/ire/mini_proj/phase1/index_folder
from nltk.stem.porter import *
from nltk.tokenize import word_tokenize, sent_tokenize
from nltk.corpus import stopwords 
from nltk.tokenize import RegexpTokenizer

from nltk.tokenize import ToktokTokenizer
from itertools import chain

from re import search, match, findall, sub, compile, finditer, DOTALL

from sys import argv
import os
import xml.sax as sx
import xml.etree.ElementTree as etree
import time

class WikiXmlHandler(sx.handler.ContentHandler):
    
    def __init__(self):
        sx.handler.ContentHandler.__init__(self)
        self._buffer = None
        self._values = {}
        self._current_tag = None
        self._pageNumber = 0
        self._doc_map = {}
        self.another_map = {}
        
    def characters(self, content): # when a character is read
        if self._current_tag:
            self._buffer.append(content)

    def startElement(self, name, attrs): # when a tag opens
        if name == 'page':
            self._pageNumber += 1
            
        if name in ('title','text'):
            self._current_tag = name
            self._buffer = []

    def endElement(self, name): # when a tag closes
        if name == self._current_tag:
            self._values[name] = ' '.join(self._buffer)        

        if name == 'page': # when page ends
            # now process the text
            self._doc_map[self._pageNumber]  = ({'id':self._pageNumber, 'title': self._values['title']})
            self.another_map[self._pageNumber]  = ({'id':self._pageNumber, 'title': self._values['title'],'body':self._values['text']})


path2dump = argv[1]

start_time = time.time()
parser = sx.make_parser()
parser.setFeature(sx.handler.feature_namespaces, 0)
handler = WikiXmlHandler()
parser.setContentHandler(handler)

while handler._pageNumber :
    print('done set ContentHandler')


parser.parse(path2dump)
# print("Parsing:  %s min ---" % ((time.time() - start_time)/60.0))

class Text_Preprocessing():
    def __init__(self, doc_map):
        self.posting_list = {}
        self.mine = ['br','\'','http','url','web','www','blp','ref','external','links']
        self.stop_words = set(stopwords.words('english')).union(self.mine)
        self.ps = PorterStemmer().stem
        self.tokenizer = RegexpTokenizer(r'[a-zA-Z0-9]+')
        self.d = doc_map
        self.t = 0
        self.toktok = ToktokTokenizer()
    
    def check(self, t1, t2, t3):
        
        if t1 not in self.posting_list:
            self.posting_list[t1] = {}
    
        if t2 not in self.posting_list[t1]:
            self.posting_list[t1][t2] = {}
            
        if t3 not in self.posting_list[t1][t2]:
            self.posting_list[t1][t2][t3] = 0
        return self.posting_list
    
    def process_title(self, text, pageNumber):
        
        text = text.lower()
        
        token_list = self.tokenizer.tokenize(text)    
        filtered_sentence = [w for w in token_list if not w in self.stop_words]
        stemmed_list = [self.ps(word) for word in filtered_sentence]
    
        for word in stemmed_list:
            self.posting_list = self.check(word, pageNumber, 't')
            self.posting_list = self.check(word, pageNumber, 'n')
            self.posting_list[word][pageNumber]['t'] += 1
            self.posting_list[word][pageNumber]['n'] += 1
  
    def process_categories(self,text, pageNumber):
        category_regex = compile(".*\[\[Category:(.*?)\]\].*")
        match_cat_list = category_regex.findall(text)
        total_stems = []
        n = len('category') + 4
        total_stems = []
        extend = total_stems.extend
        for one_match in match_cat_list:
            text = text.replace(one_match, '')
            category_name = one_match[n:-3] # say, Indian Culture
            category_name = category_name.lower()
            token_list = self.tokenizer.tokenize(category_name)
            filtered_sentence = [w for w in token_list if not w in self.stop_words]
            stemmed_list = [self.ps(word) for word in filtered_sentence]
            extend(stemmed_list)
        
        for word in total_stems: # ['data', 'scienc', 'peopl', 'birth']
            self.posting_list = self.check(word, pageNumber, 'c')
            self.posting_list = self.check(word, pageNumber, 'n')
            self.posting_list[word][pageNumber]['c'] += 1
            self.posting_list[word][pageNumber]['n'] += 1
        return text
    
    def process_infobox(self, text, pageNumber):    

        infobox_start = compile("{{Infobox")

        start_match = search(infobox_start, text)
        if start_match:

            start_pos = start_match.start()
            brack_count = 2
            end_pos = start_pos + len("{{Infobox ")
            while(end_pos < len(text)):
                if text[end_pos] == '}':
                    brack_count = brack_count - 1
                if text[end_pos] == '{':
                    brack_count = brack_count + 1
                if brack_count == 0:
                    break
                end_pos = end_pos+1

            if end_pos+1 >= len(text):
                return
            infobox_string = text[start_pos:end_pos+1]  
            text = text.replace(infobox_string, '')
            content = infobox_string.split('\n')
            content = list(map(lambda x:x.lower(),content))
            tokens = []
            add = tokens.append
            heading = content[0][len('{{infobox '):-1]
            add(heading)
            for idx in range(1,len(content)-2):
                try:
                    value = " ".join(findall(r'\w+', content[idx].split('=',1)[1])).strip()
                    add(value)
                except:
                    pass
            tokens = list(filter(lambda x: x.strip(), tokens))
            total_stems = []
            extend = total_stems.extend
            for one_token in tokens:
                token_list = one_token.split()
                filtered_sentence = [w for w in token_list if not w in self.stop_words]
                stemmed_list = [self.ps(word) for word in filtered_sentence]
                extend(stemmed_list)
            for word in total_stems:
                self.posting_list = self.check(word, pageNumber, 'i')
                self.posting_list = self.check(word, pageNumber, 'n')
                self.posting_list[word][pageNumber]['i'] += 1
                self.posting_list[word][pageNumber]['n'] += 1
        return text

    def process_ref(self, text, pageNumber):
#             pass
            ref_start = compile('< ref.* >(.*?)< /ref >', DOTALL)
            title_start = compile('.*title =|.*title=')

            tokenized_corpus = [ref_start.findall(sent) for sent in sent_tokenize(text) if len(ref_start.findall(sent))>0  ]
            tokenized_corpus = list(chain(*tokenized_corpus))
            if len(tokenized_corpus) > 4:
                tokenized_corpus = tokenized_corpus[0:4]
            total_stems = []
            extend = total_stems.extend
            for match_list in tokenized_corpus:
                text = text.replace(match_list, '')
                pipe_tokens = match_list.split('|')
                for one_token in pipe_tokens:

                    if title_start.match(one_token):

                        title = one_token.split('=')[1]
                        token_list = title.split()
                        filtered_sentence = [w.lower() for w in token_list if not w in self.stop_words]
                        stemmed_list = [self.ps(word) for word in filtered_sentence]
                        extend(stemmed_list)
            
            for word in total_stems:
                self.posting_list = self.check(word, pageNumber, 'r')
                self.posting_list = self.check(word, pageNumber, 'n')
                self.posting_list[word][pageNumber]['r'] += 1
                self.posting_list[word][pageNumber]['n'] += 1
    
    def process_body_text(self, text, pageNumber):
        
        body_ = compile('==(.*?)==|\{\{(.*?)\}\}|#(.*?)|\{\{(.*?)|\|(.*?)|\}\}|\*.*|!.*|\[\[|\]\]|;.*|&lt;.*&gt;.*&lt;/.*&gt;|<.*>.*</.*>|<.*>')
        matches = body_.findall(text)
        text = str(filter(lambda x: text.replace(x,''), matches ))
        content = text.splitlines()
        content = list(filter(lambda x: x.strip(), content))
        content = [" ".join(findall("[a-zA-Z]+", x)).strip() for x in content]
        content = list(filter(None, content)) 
        
        content = list(map(lambda x:x.lower(),content))
        
        total_stems = []
        extend = total_stems.extend
        for one_line in content:
               
            token_list = word_tokenize(one_line)
            filtered_sentence = [w for w in token_list if not w in self.stop_words]
            stemmed_list = [self.ps(word) for word in filtered_sentence]
            extend(stemmed_list)

        for word in total_stems:
            self.posting_list = self.check(word, pageNumber, 'b')
            self.posting_list = self.check(word, pageNumber, 'n')
            self.posting_list[word][pageNumber]['b'] += 1
            self.posting_list[word][pageNumber]['n'] += 1
        return text
    
    def make_index(self):
        title_regex = compile('.*:')
        for k,v in self.d.items():
            
            match_title = title_regex.match(v['title'])
            self.process_title(v['title'], v['id'])
            # if not match_title:
                # body = v['body']
                # x = self.process_categories(body, v['id'])
               
                # x = self.process_infobox(x, v['id'])
                # if x is not None:
                    # self.process_ref(x, v['id'])
                # if x is not None:
                    # x = self.process_body_text(x, v['id'])
                
                    
        return self.posting_list
    
    

'''
d[1] = {'id':1, 'title':Abc, 'body': xjse}
'''

# start_time2 = time.time()
# all_map = handler.another_map
# indexer = Text_Preprocessing(all_map)
# i = indexer.make_index()

path_to_index_folder = argv[2]

path2index = os.path.join(path_to_index_folder, 'index.txt')

# with open(path2index, 'w') as f:
#     print(i, file=f)

# path2docMap = os.path.join(path_to_index_folder, 'mapping.txt')

# with open(path2docMap, 'w') as f:
#     print(handler._doc_map, file=f)