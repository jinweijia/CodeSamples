# Question:
# Pots of gold game: Two players A & B. There are pots of gold arranged in a line, each containing 
# some gold coins (the players can see how many coins are there in each gold pot - perfect information). 
# They get alternating turns in which the player can pick a pot from one of the ends of the line. 
# The winner is the player which has a higher number of coins at the end. The objective is to "maximize" 
# the number of coins collected by A, assuming B also plays optimally. A starts the game. 

# The idea is to find an optimal strategy that makes A win knowing that B is playing optimally as well. 
# How would you do that? 

pots = [3,6,5,9,3,11,4,2]
pots2 = [6,3,1,4]
pots3 = [7,6,5,9,3,11,4,2,10]

"""Solution:
Use bottoms up dynamic programming: memoized[(i,j)] gives the maximum number of coins a player can 
obtain from a splice of the array: pots[i:j+1]. Initial length of array (even or odd) determines
whose turn it is at each step.
Base case: Array of length 1, gives either 0 or value of the pot dependent on game starter.
At each step: Two choices, pick a pot from either end of line. Maximize value from the two options
when it's your turn, minimize when not (zero-sum game).

Runtime Analysis: O(n^2) where n is length of input array
Takes O(n^2) iterations to create memoization dictionary (n-1 + n-2 + ... + 1). Each step is O(1) due 
to dictionary access. Finally takes O(n) time to retrieve final strategy.
"""
class Solution:

	def __init__(self, pots, name):
		self.pots = pots
		self.player = len(self.pots)%2
		self.mem = dict( ((i,i), k*self.player) for i, k in enumerate(pots) )
		self.coins = ['_']*len(self.pots)
		self.strategy = {}
		self.name = name


	def maximize_coins(self):

		for span in xrange(1, len(self.pots)):
			for i in xrange(len(self.pots)-span):
				if (span+1)%2 == self.player:
					x = self.mem[(i, i+span-1)]+self.pots[i+span]
					y = self.mem[(i+1, i+span)]+self.pots[i]
					if x >= y:
						self.strategy[(i, i+span)] = i+span
						self.mem[(i, i+span)] = x
					else:
						self.strategy[(i, i+span)] = i
						self.mem[(i, i+span)] = y
				else:
					x = self.mem[(i, i+span-1)]
					y = self.mem[(i+1, i+span)]
					if x < y:
						self.strategy[(i, i+span)] = i+span
						self.mem[(i, i+span)] = x
					else:
						self.strategy[(i, i+span)] = i
						self.mem[(i, i+span)] = y


	def solve(self):
		self.maximize_coins()
		a = self.mem[(0,len(self.pots)-1)]
		print self.name + " wins %d coins!" % a
		i = 0
		j = len(self.pots)-1
		while i != j:
			if (j-i+1)%2 == self.player:
				idx = self.strategy[(i, j)]
				self.coins[idx] = self.pots[idx]
			else:
				idx = self.strategy[(i, j)]
			if idx == i:
				i += 1
			else:
				j -= 1
		if self.player == 1: self.coins[i] = self.pots[i]
		print "Strategy:", self.coins

		first = self.strategy[(0, len(self.pots)-1)]
		if first == 0: return (1, len(self.pots))
		else: return (0, len(self.pots)-1)


p = pots3
a = Solution(p, 'A')
move = a.solve()
b = Solution(p[move[0]:move[1]], 'B')
b.solve()
print 'Total coins: %d' % sum(p)
# A wins 28 coins!
# Strategy: ['_', 6, 5, '_', 3, '_', 4, '_', 10]
# B wins 29 coins!
# Strategy: [7, '_', '_', 9, '_', 11, '_', 2]
# Total coins: 57