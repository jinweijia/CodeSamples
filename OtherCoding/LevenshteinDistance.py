"""Question:
Find Levenshtein distance between two sequences (minimum number of single-character edits (i.e. insertions, 
deletions or substitutions) required to change one word into the other).
"""

w1 = ['a','b','c']
w2 = ['a','c','b','d']
w3 = list('abcddefg')
w4 = list('abcdefg')
w5 = list('abraham lincoln')
w6 = list('barack hussein obama')

"""Solution:
Optimal solution can be found in Solution.optimalLD(), modifications save memory compared to first iterative 
implementation which stores whole matrix. Uses a bottom-up dp approach: M[i][j] is the Levenshtein distance 
between first i characters of string A and first j characters of string B. Memoization table is constructed 
one row at a time from left to right. Levenshtein distance of A and B will be found in M[len(A)][len(B)].

Runtime Analysis: O(ab) where a is the length of string A and b is length of (shorter) string B. Only requires 
O(b) memory since we only store two rows of the matrix.

Reference: http://en.wikipedia.org/wiki/Levenshtein_distance
"""
class Solution:

	def __init__(self, w1, w2):
		self.a = w1
		self.b = w2

	def recursiveLD(self, i, j):
		if i == len(self.a): 
			return len(self.b) - j
		elif j == len(self.b): 
			return len(self.a) - i
		if self.a[i] == self.b[j]: 
			return self.recursiveLD(i+1, j+1)
		else:
			return 1 + min(self.recursiveLD(i+1, j+1), self.recursiveLD(i+1, j), self.recursiveLD(i, j+1))

	def iterativeLD(self):
		M = [[0]*(len(self.b)+1) for _ in range(len(self.a)+1)]
		for j in xrange(len(self.b)):
			M[0][j+1] = j+1
		for i in xrange(len(self.a)):
			M[i+1][0] = i+1
		for i in xrange(1,len(self.a)+1):
			for j in xrange(1,len(self.b)+1):
				prev = M[i-1][j-1]
				if self.a[i-1] == self.b[j-1]:
					M[i][j] = prev
				else:
					M[i][j] = min(M[i][j-1], M[i-1][j], prev) + 1
		return M[len(self.a)][len(self.b)]

	def optimalLD(self):
		if self.a == self.b: return 0

		A = self.a
		B = self.b
		if len(self.a) < len(self.b):
			A = self.b
			B = self.a
			
		previous = range(len(B)+1)
		for i in xrange(1,len(A)+1):
			current = [i] + [0]*len(B)
			for j in xrange(1,len(B)+1):
				prev = previous[j-1]
				if A[i-1] == B[j-1]:
					current[j] = prev
				else:
					current[j] = min(current[j-1], previous[j], prev) + 1
			previous = current
		return current[len(B)]

	def solve(self, method='optimal'):
		if method == 'recursive':
			ans = self.recursiveLD(0, 0)
		elif method == 'iterative':
			ans = self.iterativeLD()
		elif method == 'optimal':
			ans = self.optimalLD()
		print "Minimum number of edits: %d" % ans


# s = Solution(w1, w2) # 2
# s = Solution(w3, w4) # 1
# s = Solution(w5, w6) # 14
# s = Solution(list('kitten'), list('sitting')) # 3
# s = Solution(list('saturday'), list('sunday')) # 3
s = Solution(list('abcddzefgh'), list('abcdzefgh'))

s.solve()