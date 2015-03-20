"""Question: Longest Increasing Subsqquence
Find a subsequence of a given sequence in which the subsequence's elements are in sorted order, 
lowest to highest, and in which the subsequence is as long as possible. No duplicates should be in
the final subsequence.
"""

import math

lst = [10, 22, 9, 33, 21, 50, 41, 60, 80]
lst2 = [0, 8, 4, 12, 2, 10, 6, 14, 1, 9, 5, 13, 3, 11, 7, 15]
lst3 = [8, 8, 10, 10, 10]

"""Solution:
Optimal solution can be found in Solution.solve_optimal(). Since X[M[i]] is strictly increasing as i increases, 
at each iteration we can use binary search to obtain the length of the LIS whose termination value is still 
smaller than the value at current array index. Increment this length to obtain the length of LIS terminating at 
current index.

Runtime Analysis: O(nlogn) where n is length of input sequence.
There are n iterations which take O(logn) time each for binary search.

Reference: http://en.wikipedia.org/wiki/Longest_increasing_subsequence
"""
class Solution:

	def __init__(self, arr):
		self.sequence = arr
		self.M = [0]*len(self.sequence)
		self.P = [-1]*len(self.sequence) # predecessors
		self.L = 0
		self.lastidx = 0

	# Bottom-up dynamic programming, M[i] is length of the LIS in the first i characters of the array. Total: O(n^2)
	def solve_dp(self):
		self.M[0] = 1
		for i in xrange(1,len(self.sequence)):
			L_i = 1
			for j in xrange(i):
				if self.sequence[j] <  self.sequence[i]:
					if self.M[j] + 1 > L_i:
						L_i = self.M[j] + 1
						self.P[i] = j
			if L_i > self.L: 
				self.L = L_i
				self.lastidx = i
			self.M[i] = L_i
		return self.L

	# Uses binary searching, M[i] is index of the smallest termination value of an increasing subsequence with length i.
	def solve_optimal(self):
		for i in xrange(len(self.sequence)):
			lo = 0
			hi = self.L-1
			while lo <= hi:
				mid = int(math.ceil((lo+hi)/2.0))
				if self.sequence[self.M[mid]] < self.sequence[i]:
					lo = mid + 1
				else:
					hi = mid - 1
			L_i = lo
			self.M[L_i] = i
			if L_i == 0:
				self.P[i] = -1
			else:
				self.P[i] = self.M[L_i-1]
			if L_i+1 > self.L:
				self.L = L_i+1
		self.lastidx = self.M[self.L-1]
		return self.L

	# Prints the LIS
	def lis(self):
		i = self.L-1
		idx = self.lastidx
		s = [0]*self.L
		while i > -1:
			s[i] = self.sequence[idx]
			idx = self.P[idx]
			i -= 1
		return s


s = Solution(lst2)
l = s.solve_optimal()
print "Length %d:" % l, s.lis()
# Length 6: [0, 4, 6, 9, 13, 15]