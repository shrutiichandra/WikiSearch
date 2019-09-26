from nltk.corpus import stopwords 
from nltk.tokenize import word_tokenize, sent_tokenize
# from nltk.stem.porter import *
from nltk.stem.snowball import SnowballStemmer
from nltk.tokenize import RegexpTokenizer
from nltk.tokenize import ToktokTokenizer
from re import search, match, findall, sub, compile, finditer, DOTALL, split, escape
import nltk.data
from itertools import chain
import time
class Text_Preprocessing():
    def __init__(self, doc_map):
        self.posting_list = {}
        self.mine = ['br','\'','http','url','web','www','blp','ref','external','links']
        self.stop_words = set(stopwords.words('english')).union(self.mine)
        # self.ps = PorterStemmer().stem
        self.ps = SnowballStemmer("english").stem

        self.tokenizer = RegexpTokenizer(r'[a-zA-Z]+|[0-9]{,4}')
        self.d = doc_map
        self.sent = nltk.data.load('tokenizers/punkt/english.pickle').tokenize         
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
        

        token_list = self.tokenizer.tokenize(text.lower())    
        token_list = list(filter(None, token_list)) 

        filtered_sentence = [w for w in token_list if not w in self.stop_words]
        stemmed_list = [self.ps(word) for word in filtered_sentence if len(word)<11]
        stemmed_list = list(filter(None, stemmed_list))         
        # print('stemmedList title: ',stemmed_list)
        for word in stemmed_list:
            
            self.posting_list = self.check(word, pageNumber, 't')
            self.posting_list = self.check(word, pageNumber, 'n')
            self.posting_list[word][pageNumber]['t'] += 1
            self.posting_list[word][pageNumber]['n'] += 1
  
    def process_categories(self,text, pageNumber):
        c = 0
        category_regex = compile(".*\[\[Category:(.*?)\]\].*")
        match_cat_list = category_regex.findall(text)
        total_stems = []
        n = len('category') + 4
        total_stems = []
        rem = '[[Category:%s]]'
        extend = total_stems.extend
        for one_match in match_cat_list[:4]:
        
            text = text.replace(rem%(one_match), '')
            category_name = one_match[n:-3] # say, Indian Culture
            category_name = category_name.lower()
            token_list = self.tokenizer.tokenize(category_name)
            token_list = list(filter(None, token_list)) 

            filtered_sentence = [w for w in token_list if not w in self.stop_words]
            stemmed_list = [self.ps(word) for word in filtered_sentence if len(word)<11]
            extend(stemmed_list)
            
        
        for word in total_stems: # ['data', 'scienc', 'peopl', 'birth']
            # if word == '':
            #     print('here null category')
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
            tokens = list(filter(None, tokens)) 
            total_stems = []
            extend = total_stems.extend
            for one_token in tokens:
                token_list = self.tokenizer.tokenize(one_token)
                filtered_sentence = [w for w in token_list if not w in self.stop_words]
                stemmed_list = [self.ps(word) for word in filtered_sentence if len(word)<11]
                extend(stemmed_list)
            total_stems = list(filter(None, total_stems)) 
            for word in total_stems:
                # if word == '':
                #     print('here null ibox; ', total_stems)
                self.posting_list = self.check(word, pageNumber, 'i')
                self.posting_list = self.check(word, pageNumber, 'n')
                self.posting_list[word][pageNumber]['i'] += 1
                self.posting_list[word][pageNumber]['n'] += 1
        return text

    def process_ref(self, text, pageNumber):
#             pass
            ref_start = compile('< ref.* >(.*?)< /ref >', DOTALL)
            title_start = compile('.*title =|.*title=')
            n=2
            tokenized_corpus = [ref_start.findall(sent) for sent in sent_tokenize(text) if len(ref_start.findall(sent))>0  ]
            tokenized_corpus = list(chain(*tokenized_corpus))
            if len(tokenized_corpus) > n:
                tokenized_corpus = tokenized_corpus[:n]
            total_stems = []
            extend = total_stems.extend
            # print('ref len %f'%len(tokenized_corpus))
            for match_list in tokenized_corpus:
                text = text.replace(match_list, '')
                pipe_tokens = match_list.split('|')
                for one_token in pipe_tokens:

                    if title_start.match(one_token):

                        title = one_token.split('=')[1]
                        token_list = self.tokenizer.tokenize(one_token)
                        filtered_sentence = [w.lower() for w in token_list if not w in self.stop_words]
                        stemmed_list = [self.ps(word) for word in filtered_sentence]
                        stemmed_list = list(filter(None, stemmed_list)) 
                        extend(stemmed_list)
            
            for word in total_stems:
                self.posting_list = self.check(word, pageNumber, 'r')
                self.posting_list = self.check(word, pageNumber, 'n')
                self.posting_list[word][pageNumber]['r'] += 1
                self.posting_list[word][pageNumber]['n'] += 1
    
    def process_body_text(self, text, pageNumber):
        
        body_ = compile(r'==(.*)==|{{(.*)}}|#(.*)|{{(.*)|{{(.*)|\|(.*)|\}\}|\*.*|!.*|\[\[|\]\]|;.*|&lt;.*&gt;.*&lt;/.*&gt;|<.*>.*</.*>|<.*>')
        matches = list(chain.from_iterable(body_.findall(text)))

        matches = list(filter(None, matches)) 
        # text = filter(lambda x: text.replace(x,''), matches )
        big_regex = compile('|'.join(map(escape, matches)))
        text = big_regex.sub('',text)
        
        
        content = text.splitlines()
        content = list(filter(lambda x: x.strip(), content))

        content = [" ".join(findall("[a-zA-Z]+", x)).strip() for x in content]
        content = list(filter(None, content)) 
        
        content = list(map(lambda x:x.lower(),content))
        
        total_stems = []
        extend = total_stems.extend
        if len(content)>200:
            for one_line in range(0,len(content),5):
                   
                token_list = word_tokenize(content[one_line])
                filtered_sentence = [w for w in token_list if not w in self.stop_words]
                stemmed_list = [self.ps(word) for word in filtered_sentence]
                extend(stemmed_list)
        else:
            for one_line in content:
                   
                token_list = word_tokenize(one_line)
                filtered_sentence = [w for w in token_list if not w in self.stop_words]
                stemmed_list = [self.ps(word) for word in filtered_sentence]
                extend(stemmed_list)
        
        for word in total_stems:
            # if word == '':
                # print('here null boy')
            self.posting_list = self.check(word, pageNumber, 'b')
            self.posting_list = self.check(word, pageNumber, 'n')
            self.posting_list[word][pageNumber]['b'] += 1
            self.posting_list[word][pageNumber]['n'] += 1
        return text

#     def process_ref(self, text, pageNumber):
# #             pass
#             ref_regex = compile('.*< ref (.*?)< /ref >.*',DOTALL)
#             ref_tag = ref_regex.findall(text)
#             i = 0
            
#                 # title_start = compile('(.*?)title =|(.*?)title=')
#             for r in ref_tag:

#                 try:
#                     i+=1
#                     if i==4:
#                         break
#                     text = text.replace('< ref '+r+'< /ref >', '')
#                     r = split(r'title',r)[1].split('|',1)[0].replace('=','').strip()
                    
#                     token_list = self.tokenizer.tokenize(r)

#                     filtered_sentence = [w.lower() for w in token_list if not w in self.stop_words]

#                     stemmed_list = [self.ps(word) for word in filtered_sentence if len(word)<11]
#                     extend(stemmed_list)

#                     for word in total_stems:
#                         self.posting_list = self.check(word, pageNumber, 'r')
#                         self.posting_list = self.check(word, pageNumber, 'n')
#                         self.posting_list[word][pageNumber]['r'] += 1
#                         self.posting_list[word][pageNumber]['n'] += 1
#                 except:
#                     pass
                    
        
#     # def ab_with_check(self,text):
#     #     for ch in ['\\','`','*','_','{','}','[',']','(',')','>','#','+','-','.','!','$','\'']:
#     #         if ch in text:
#     #             text = text.replace(ch,"\\"+ch)
    
#     def process_body_text(self, text, pageNumber):
        
#         body_ = compile('==.*==|\{\{.*\}\}|#.*|\{\{.*|\|.*|\}\}|\*.*|!.*|\[\[|\]\]|;.*|&lt;.*&gt;.*&lt;/.*&gt;|<.*>.*</.*>|<.*>')
#         matches = set(body_.findall(text))
#         # print(matches)
#         # text = text.replace(x,'') for x in matches
#         # print(text)

#         if matches:
            
#             for one_match in matches:
#                 # print('one_match: ',one_match)
#                 text = text.replace(one_match,'')
#                 # print('text: ',text)
        
#         # text = str(filter(lambda x: text.replace(x,''), matches ))
#         content = text.splitlines()
#         content =[" ".join(findall("[a-zA-Z]+", x)).strip().lower() for x in content]
#         # content = [x.strip() for x in content]

#         # content = [" ".join(findall("[a-zA-Z]+", x)).strip() for x in content]
#         content = list(filter(None, content)) 
#         # print(content)

#         # content = list(map(lambda x:x.lower(),content))
#         # # content = " ".join(content)
#         # # print(content)s
#         total_stems = []
#         extend = total_stems.extend
#         if len(content)>200:
            
#             for one_word in range(0,len(content),2):
                   
#                 token_list = self.tokenizer.tokenize(content[one_word])
#                 filtered_sentence = [w for w in token_list if not w in self.stop_words]
#                 stemmed_list = [self.ps(word) for word in filtered_sentence if len(word)<20]
#                 extend(stemmed_list)
#         else:
#             for one_word in content:
        
#                 token_list = self.tokenizer.tokenize(one_word)
#                 filtered_sentence = [w for w in token_list if not w in self.stop_words]
#                 stemmed_list = [self.ps(word) for word in filtered_sentence if len(word)<20]
#                 extend(stemmed_list)

#         for word in total_stems:
#             self.posting_list = self.check(word, pageNumber, 'b')
#             self.posting_list = self.check(word, pageNumber, 'n')
#             self.posting_list[word][pageNumber]['b'] += 1
#             self.posting_list[word][pageNumber]['n'] += 1
#         return text
    
    def make_index(self):
        limit_one_doc = 30/60000.0 # in sec
        # print('make index')
        title_regex = compile('.*?:')
        for k,v in self.d.items():
            
            t1,t2,t3,t4,t5=0,0,0,0,0
            t = time.time() 
            match_title = title_regex.match(v['title'])
            self.process_title(v['title'], v['id'])            
            t1= time.time()-t
            if not match_title:
                body = v['body']
                t = time.time()
                x = self.process_categories(body, v['id'])
                t2= time.time()-t
                t= time.time()
                x = self.process_infobox(x, v['id'])
                t3= time.time()-t
                if x is not None:
                    # t = time.time()
                    self.process_ref(x, v['id'])
                t4=0
                if x is not None:
                    t = time.time()
                    
                    x = self.process_body_text(x, v['id'])
                    t5= time.time()-t
            T = t1+t2+t3+t4+t5
            if T>=limit_one_doc:
                pass
                # print('id %s title %f cat %f infobox %f ref %f body %f' % (v['id'],t1,t2,t3,t4,t5))
                # print('--> T: %f limit: %f exceed: %f'%(T, limit_one_doc, T-limit_one_doc))
            # print(i,end=' ')
                    
        return

    def parse_posting_list(self, path2index):
        complete_index = dict(sorted(self.posting_list.items()))

        for term, posting_list in complete_index.items():
            # if s:
            #     print('term: ',term)
            # if term == '':
            #     print()
            one_line = ""
            one_line = term + "|"
            for doc_id, occurences in posting_list.items(): 
                one_line += str(doc_id) + "$"
                
                for field, count in occurences.items():
                    one_line += field + ":" + str(count) + "#"
                    
                one_line += "|"
            one_line += "\n"
            with open(path2index, 'a+') as i:
                i.write(one_line)
    #one line: 0|29$i:1#n:1#|61$i:1#n:1#|..