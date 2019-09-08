from heapq import heappush, heappop, heapify 

class MinHeap: 
	
	def __init__(self):
		self.heap = []

	def __lt__(self, other):
		return self.f < other.f
	def parent(self, i): 
		return (i-1)/2
	

	def insertKey(self, item): 
		
		heappush(self.heap, item)		 


	
	def extractMin(self): 
		return heappop(self.heap) 

# l = ["0", {"10027": {"i": 5, "n"/s