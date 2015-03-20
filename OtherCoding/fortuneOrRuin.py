"""Question: 
Johnny is a gambler, and he really likes to play for high stakes in casinos. Recently,
his favorite casino is advertising a new betting game, where one can possibly win a fortune! (What
they are not advertising is that you can also lose everything.) The rules are the following: the
player bets a certain amount of money in each round of the game. If the player wins in this round,
he wins back his bet and on top of this, another half of the size of the bet. However if the player
loses, he regains (only) two thirds of his bet. To resolve a single round, a very complicated
procedure is used, involving roulette, card dealing, and dice rolls. However, one thing can be
stated for sure: in each round, the player has exactly 50% chance of winning and 50% chance of
losing.

Johnny is extremely dedicated to his cause, so once he starts playing, he will never stop as long as
he is allowed to continue, and he will always bet his entire stake in the next round. However, the
casino has imposed some restrictions. First, there is a minimal size of the bet. Then, there is a
luck detection system: if the player ends a round (after winning) with more than a certain amount of
cash, he is considered 'too lucky' and prohibited from continuing to play the game. Given the
starting amount of cash Johnny has, the minimal size of the bet, and the cap on winnings imposed by
the casino, determine which of the two scenarios is more probable: player's ruin (dropping below the
minimum bet size), or player's fortune (exceeding the cap imposed by the casino). Or maybe they are
equally probable? """ 

# First solution, not optimal, recurses down the tree of probabilities to calculate probability of winning.
class FortuneOrRuin:

	def __init__(self, c, l, u):
		self.cash = c
		self.lower = l
		self.upper = u
		self.seenprobs = {}
		self.scales = {}
		self.p = 0

	def findp(self, cash, depth):
		# print 'findp: %.2f, %d' % (cash, depth)
		if cash < self.lower:
			return 0.0
		elif cash > self.upper:
			return 1.0
		if cash in self.seenprobs:
			if self.seenprobs[cash] >= 0.0:
				return self.seenprobs[cash]
			else:
				self.scales[cash] -= 2**-depth
				return 0
		else: 
			self.seenprobs[cash] = -1.0
			self.scales[cash] = 1.0
			self.seenprobs[cash] = (0.5*self.findp(1.5*cash, depth+1) + 0.5*self.findp(2*cash/3, depth+1)) * 1/self.scales[cash]
			# print 'findp: %.2f, %d -> %.4f' % (cash, depth, self.seenprobs[cash])
			return self.seenprobs[cash]

	def printresult(self):
		p = self.findp(self.cash, 0)
		if p == 1-p:
			print 'A fair game'
		elif 2*p > 1:
			print 'Player fortune'
		else:
			print 'Player ruin'


# game = FortuneOrRuin(8.0, 1.0, 10000.0)
# game.printresult()

# print game.seenprobs
# print game.scales
import math

# Optimal solution
def findOutcome(cash, upper, lower):
	lcount = math.ceil(math.log(lower / cash) / math.log (2.0/3))
	ucount = math.ceil(math.log(upper / cash) / math.log (3.0/2))
	if lcount == ucount:
		print 'A fair game'
	elif lcount > ucount:
		print 'Player fortune'
	else:
		print 'Player ruin'



for _ in range(input()):
	cash, lower, upper = [float(x) for x in raw_input().split()]
	# game = FortuneOrRuin(cash, lower, upper)
	# game.printresult()
	findOutcome(cash, upper, lower)