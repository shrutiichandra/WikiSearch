
#bash search.sh <path_to_index_folder> <path_to_input_query_file> <path_to_output_file>

import searcher as srch

import sys
import os

def read_file(testfile):
    with open(testfile, 'r') as file:
        queries = file.readlines()
    return queries


def write_file(outputs, path_to_output):
    '''outputs should be a list of lists.
        len(outputs) = number of queries
        Each element in outputs should be a list of titles corresponding to a particular query.'''
    # print('write ',path_to_output)
    with open(path_to_output, 'w') as file:
        for output in outputs:
            for line in output:
                file.write(line.strip() + '\n')
            file.write('\n')


def search(path_to_index, queries):
    
    '''Write your code here'''
    # print('here')

    index = srch.read_index(path_to_index)
    outputs = []
    add = outputs.append
    queries = [x[:-1] for x in queries]
    index_file = path_to_index
    for one_query in queries:
            q = srch.Query_PreProcess(one_query, index)
            results = q.search()
            add(results)
    return outputs
    #pass


def main():
    #path_to_index = sys.argv[1]
    #testfile = sys.argv[2]
    #path_to_output = sys.argv[3]
    path_to_index_folder = sys.argv[1]   
    path_to_index = os.path.join(path_to_index_folder,'index.txt')
    testfile = sys.argv[2] # input query file
    path_to_output = sys.argv[3]
    path_to_mapping_file = os.path.join(path_to_index_folder,'mapping.txt')
    srch.read_mapping(path_to_mapping_file)
    queries = read_file(testfile)
    outputs = search(path_to_index, queries)
    write_file(outputs, path_to_output)

if __name__ == '__main__':
    main()