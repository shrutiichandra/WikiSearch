#!/usr/bin/env python
# coding: utf-8

# In[1]:


filename = 'enwiki-latest-pages-articles26.xml-p42567204p42663461'


# In[3]:


# from nltk.stem import PorterStemmer 
from nltk.stem.porter import *
from nltk.tokenize import word_tokenize, sent_tokenize
from nltk.corpus import stopwords 
from nltk.tokenize import RegexpTokenizer


# In[4]:


from nltk.tokenize import ToktokTokenizer
from itertools import chain


# In[5]:


from re import search, match, findall, sub, compile, finditer, DOTALL


# In[ ]:


import gc


# In[6]:


import xml.sax as sx
import time

class WikiXmlHandler(sx.handler.ContentHandler):
    
    def __init__(self):
        sx.handler.ContentHandler.__init__(self)
        self._buffer = None
        self._values = {}
        self._current_tag = None
        self._pages = [] #[(title1, body1), (title2, body2)...]
        self._content = {}
        self._pageNumber = 0
#         self._pageNum_title_map = {}  # {0: Kim Hyeon , 1: Marko Virtanen..}
        self._doc_map = {}
        self.posting_list = {}
        self.categories_list = {}
        self.count = {}
        
    def characters(self, content): # when a character is read
        global alphabets
        if self._current_tag:
#             match = alphabets.match(content)
#             if match:
            self._buffer.append(sub(r'\b\d+(?:\.\d+)?\s+', '', content))

    def startElement(self, name, attrs): # when a tag opens
        if name == 'page':
            self._pageNumber += 1
            
        if name in ('title', 'text'):
            self._current_tag = name
            self._buffer = []

    def endElement(self, name): # when a tag closes
        if name == self._current_tag:
            self._values[name] = ' '.join(self._buffer)        

        if name == 'page': # when page ends
            # now process the text
            content = self._values['text']
#             self.process_text(content)
            self._doc_map[self._pageNumber]  = ({'id':self._pageNumber, 'title': self._values['title'], 'body': content})


# In[7]:


start_time = time.time()
parser = sx.make_parser()
parser.setFeature(sx.handler.feature_namespaces, 0)
handler = WikiXmlHandler()
parser.setContentHandler(handler)
parser.parse('../../../dataset/enwiki-latest-pages-articles26.xml-p42567204p42663461')
print("Parsing:  %s min ---" % ((time.time() - start_time)/60.0))


# In[27]:


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
        flag = False
        text = text.lower()
        
        token_list = self.tokenizer.tokenize(text)
        
        filtered_sentence = [w for w in token_list if not w in self.stop_words]
        stemmed_list = [self.ps(word) for word in filtered_sentence]
        #     token_list = tokenize(text)
        #     stemmed_list = stemmer(token_list)
#         print('title: ',filtered_sentence)
        for word in stemmed_list:
            self.posting_list = self.check(word, pageNumber, 't')
            self.posting_list = self.check(word, pageNumber, 'n')
            self.posting_list[word][pageNumber]['t'] += 1
            self.posting_list[word][pageNumber]['n'] += 1
  
    def process_categories(self,text, pageNumber):
        category_regex = compile(".*\[\[Category:.*\]\].*")
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
#             print(infobox_string)
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
#                 print('infobox: ',filtered_sentence)
                stemmed_list = [self.ps(word) for word in filtered_sentence]
                extend(stemmed_list)
            for word in total_stems:
#                     print(word)
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
#             print(tokenized_corpus)
#             print('------------')
            if len(tokenized_corpus) > 4:
                tokenized_corpus = tokenized_corpus[0:4]
#             print(tokenized_corpus)
            total_stems = []
            extend = total_stems.extend
            for match_list in tokenized_corpus:
#                 match_list = ref_start.findall(one_sentence)
#                 print(match_list)
                text = text.replace(match_list, '')
                pipe_tokens = match_list.split('|')
                for one_token in pipe_tokens:

                    if title_start.match(one_token):

                        title = one_token.split('=')[1]
#                             print(title)

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
        
        body_ = compile('==.*==|\{\{.*\}\}|#.*|\{\{.*|\|.*|\}\}|\*.*|!.*|\[\[|\]\]|;.*|&lt;.*&gt;.*&lt;/.*&gt;|<.*>.*</.*>|<.*>')
        matches = body_.findall(text)
        text = str(filter(lambda x: text.replace(x,''), matches ))
#         text = sub(body_, '', text)
#         text = text.replace(body_,'')
#         print(text)
#         print('----------------')
        content = text.splitlines()
        content = list(filter(lambda x: x.strip(), content))
#         content = self.tokenizer.tokenize(text)
        content = [" ".join(findall("[a-zA-Z]+", x)).strip() for x in content]
        content = list(filter(None, content)) 
        
        content = list(map(lambda x:x.lower(),content))
        
        total_stems = []
        extend = total_stems.extend
        for one_line in content:
               
            token_list = word_tokenize(one_line)
            filtered_sentence = [w for w in token_list if not w in self.stop_words]
            #                 print('body: ',filtered_sentence)
            stemmed_list = [self.ps(word) for word in filtered_sentence]
            extend(stemmed_list)
#         print(total_stems)
        for word in total_stems:
            self.posting_list = self.check(word, pageNumber, 'b')
            self.posting_list = self.check(word, pageNumber, 'n')
            self.posting_list[word][pageNumber]['b'] += 1
            self.posting_list[word][pageNumber]['n'] += 1
        return text
    
    def make_index(self):
        title_regex = compile('.*:')
        for k,v in self.d.items():
            start_time = time.time()
            print('processing no ', k, end = ', ')
            match_title = title_regex.match(v['title'])
            self.process_title(v['title'], v['id'])
            if not match_title:
                body = v['body']
                x = self.process_categories(body, v['id'])
#                 print('categories done ',end = ', ')
                x = self.process_infobox(x, v['id'])
#                 print('infobox done ',end = ', ')
                if x is not None:
                    self.process_ref(x, v['id'])
#                 print('references done', end = ', ')
                if x is not None:
                    x = self.process_body_text(x, v['id'])
#                 print('body done ')
                a = time.time() - start_time
                if a >= 0.02:
                    self.t += a
                    
        return self.posting_list
    def make_index_n(self,n):
        start= time.time()
#         print('here')
        title_regex = compile('.*:|.*;')
        num = n
        v = self.d[n]
#         print('title: ',v['title'])
        match_title = title_regex.match(v['title'])
        self.process_title(v['title'], num)
        print('title done')
        if not match_title:
            body = v['body']
            x = self.process_categories(body, num)
            print('categories done ',end = ', ')
            x = self.process_infobox(x, num)
            print('infobox done ',end = ', ')
            if x is not None:
                self.process_ref(x, num)
            print('references and external links done', end = ', ')
            if x is not None:
                x = self.process_body_text(x, num)
            print('body done ')
        print(time.time() - start,' seconds')
            
        return self.posting_list


# In[28]:


'''
d[1] = {'id':1, 'title':Abc, 'body': xjse}
'''

start_time2 = time.time()
d = handler._doc_map
indexer = Text_Preprocessing(d)
i = indexer.make_index()
# i = indexer.make_index_n(6)
print("--- %s min ---" % ((time.time() - start_time2)/60.0))


# In[263]:


def find_titles(keyword):
    global d
    global i
    doc_ids_dict = i[keyword]
    for key,val in doc_ids_dict.items():
        print(d[key]['title'])


# In[30]:


with open('../res/index/index.txt', 'w') as f:
    print(i, file=f)


# In[287]:


with open('mapping.txt', 'w') as f:
    print(handler._doc_map, file=f)

