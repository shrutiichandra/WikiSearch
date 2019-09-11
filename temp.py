import json

d1 = {1:[2], 2:[3],3:[4],4:[5],5:[6],7:[8],9:[10],11:[20],19:[15],20:[18]}
d2 = {1:[4],2:[6],7:[9],10:[13],14:[15],16:[18],20:[30],21:[15],22:[18],23:[2],24:[6]}
# s = pd.Series(d1, name='list')
# print(s)
# s.index.name='key'
# s.reset_index()

with open("d1.txt",'w') as t:
	# d1={ k:v' for k,v in d1.items()}  
	json.dump(d1,t)
with open("d2.txt",'w') as t:
	json.dump(d2,t)


with open("d1.txt",'r') as t:
	# d1={ k:v' for k,v in d1.items()}  
	d = json.load(t)
print(list(d.items())[0:4])
# with open("d2.txt",'w') as t:
# 	json.dump(d2,t)
# def union_collections(d1, d2):
#     union = {}

#     for key in set(d1.keys()).union(d2.keys()):
#         if key in d1 and key not in d2: # if the key is only in d1
#             union[key] = d1[key]

#         if key in d2 and key not in d1: 
#             union[key] = d2[key]

#         if key in d1 and key in d2:
#             union[key] = sorted(list(set(d1[key] + d2[key])))

#     return union
# # print(union_collections(d1,d2))
# begin = 0
# offset = 4
# lines = [] # list of list
# while True:
# 	temp_line = []
# 	for i in range(2):

# 		with open("d"+str(i+1)+".txt",'r') as t:

# 			print('reading d',i+1)
# 			print('offset: ',offset, ' begin: ',begin)
# 			line = t.readlines()[begin:offset]
# 			print(line)
# 			line = list(map(lambda each:each.strip('\n'), line))
# 			temp_line.extend(line)
# 	print('>>>combined line: ',temp_line)
# 	# sort this line



# 	print('----end for---')
# 	if not temp_line:
# 		print('reached end')
# 		break
# 	begin = offset + 1
# 	offset += 4
# 	lines.append(temp_line)

# print('end while///')

# print(lines)


# import heapq/