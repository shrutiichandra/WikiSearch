#!/usr/bin/env python
# coding: utf-8

# In[ ]:


from .searcher import Query_PreProcess


# In[ ]:


import sys


def read_file(testfile):
    with open(testfile, 'r') as file:
        queries = file.readlines()
    return queries


def write_file(outputs, path_to_output):
    '''outputs should be a list of lists.
        len(outputs) = number of queries
        Each element in outputs should be a list of titles corresponding to a particular query.'''
    with open(path_to_output, 'w') as file:
        for output in outputs:
            for line in output:
                file.write(line.strip() + '\n')
            file.write('\n')


def search(path_to_index, queries):
    '''Write your code here'''
    outputs = []
    add = outputs.append
    queries = [x[:-1] for x in queries]
    index_file = path_to_index
    for one_query in queries:
            q = Query_PreProcess(one_query, index_file)
            results = q.search()
            add(results)
    return outputs
    #pass


def main():
    #path_to_index = sys.argv[1]
    #testfile = sys.argv[2]
    #path_to_output = sys.argv[3]
    path_to_index = '../res/index/index.txt'
    testfile = '../../../sampleQueriesAndResults/queryfile'
    path_to_output = '../../../sampleQueriesAndResults/output'
    queries = read_file(testfile)
#     print(queries)
#     print(type(queries))
    outputs = search(path_to_index, queries)
    write_file(outputs, path_to_output)
#     print('hi')


# In[ ]:


if __name__ == '__main__':
    main()

