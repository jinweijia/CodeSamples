# Question:
# You have k lists of sorted integers. Find the smallest range that includes at least one number from each of the k lists. 

# For example, 
# List 1: [4, 10, 15, 24, 26] 
# List 2: [0, 9, 12, 20] 
# List 3: [5, 18, 22, 30] 

# The smallest range here would be [20, 24] as it contains 24 from list 1, 20 from list 2, and 22 from list 3.

l = []
l.append([4, 10, 15, 24, 26])
l.append([0, 9, 12, 20])
l.append([5, 18, 22, 30])
k = 3

"""Solution:
Start at first element of each array.
While we have not reached the end of any array:
    Find the range of the current element set.
    Check against global smallest.
    Discard smallest element and get the next element from same array.

Runtime Analysis: O(Nlogk) where N is sum of array lengths
Use min heap so we can successively retrieve smallest element. While loop runs for at most N times, 
each step takes logk time for maintaining min heap invariant (parents must be larger than children).
"""
import heapq # min heap implementation

smallest = float("inf")
largest = float("-inf")
pointer = []   # each element in pointer consists of (value, list #, index)
    
def get_range(): # gets the range of the current set of k numbers. O(1)
    global smallest, pointer, largest    
    r = largest - pointer[0][0]
    if r < smallest:
        smallest = r

def update(lnum, i): # increment index of array with smallest value, returns true if at the last element of array. O(log k)
    global l, pointer, largest
    s = l[lnum]
    if i < len(s)-1:
        val = s[i+1]
        if val > largest:
            largest = val
        heapq.heapreplace(pointer, (val,lnum,i+1))        
        return False
    return True
    
def find_smallest_range(l): # runs at most N (total # of elements) times. solution runtime: O(N*log k)
    global smallest, pointer, largest
    # initialize
    for i in range(k):
        heapq.heappush(pointer, (l[i][0], i, 0))
    largest = heapq.nlargest(1, pointer)[0][0]
    get_range()

    while True:
        if update(pointer[0][1], pointer[0][2]):
            break
        else:
            get_range()
        
    return (pointer[0][0], heapq.nlargest(1, pointer)[0][0])

print find_smallest_range(l)
# (20, 24)
