package simpledb;

/** A class to represent a fixed-width histogram over a single integer-based field.
 */
public class IntHistogram {

    /**
     * Create a new IntHistogram.
     * 
     * This IntHistogram should maintain a histogram of integer values that it receives.
     * It should split the histogram into "buckets" buckets.
     * 
     * The values that are being histogrammed will be provided one-at-a-time through the "addValue()" function.
     * 
     * Your implementation should use space and have execution time that are both
     * constant with respect to the number of values being histogrammed.  For example, you shouldn't 
     * simply store every value that you see in a sorted list.
     * 
     * @param buckets The number of buckets to split the input value into.
     * @param min The minimum integer value that will ever be passed to this class for histogramming
     * @param max The maximum integer value that will ever be passed to this class for histogramming
     */
    
    /* Total number of buckets in the histogram */
    int _buckets;

    /* Maximum integer value in histogram */
    int _max;

    /* Minimum integer value in histogram */
    int _min;

    /* Linear integer range of histogram, ie max - min */
    int _range;
    
    /* The integer interval each bucket covers, which is width of bucket */
    int _interval;
    
    /* Each entry in _histogramBuckets correspond to count of number of items in the bucket*/
    int[] _histogramBuckets;

    /* Total number of tuples inserted into the histogram */ 
    int _numTuples = 0;

    public IntHistogram(int buckets, int min, int max) {
	_buckets = buckets;
	_min = min;
	_max = max;
	_range = max-min;
    
	initializeBuckets();
    }

    /**
     * Add a value to the set of values that you are keeping a histogram of.
     * @param v Value to add to the histogram
     */
    public void addValue(int v) {

	//Find index of bucket to put v in
	int index = getBucketIndex(v);
	
	//Increment the count of that bucket
	int i = _histogramBuckets[index]+1; 	
	_histogramBuckets[index] = i;      
	
	//Increment the number of Tuples in this histogram
	_numTuples++;
       
    }

    /**
     * Estimate the selectivity of a particular predicate and operand on this table.
     * 
     * For example, if "op" is "GREATER_THAN" and "v" is 5, 
     * return your estimate of the fraction of elements that are greater than 5.
     * 
     * @param op Operator
     * @param v Value
     * @return Predicted selectivity of this particular operator and value
     */
    public double estimateSelectivity(Predicate.Op op, int v) {

	// BucketIndex is index of the bucket v is in
	int bucketIndex  = getBucketIndex(v);

	double countsum = 0;
       
	switch(op){

	// For both greater than operations, we need to add the count from all the buckets
	// with range larger than the targetBucket
	case GREATER_THAN_OR_EQ:
	case GREATER_THAN:
	    if (v < _min) return 1.0;
	    if (v > _max) return 0.0;

	    for(int i = bucketIndex +1; i <= _buckets-1; i++){
		countsum += _histogramBuckets[i];
	    }
	    break;

	// For both less than operations, we need to add the count from all the buckets
	// with range less than the targetBucket
	case LESS_THAN_OR_EQ:
	case LESS_THAN:
	    if (v <= _min) return 0.0;
	    if (v > _max) return 1.0;

	    for(int i = bucketIndex; i >= 0; i--){
		countsum += _histogramBuckets[i];
	    }
	    break; 
	 // For equality, we need to add the count of the target Bucket
	case EQUALS:
	    if (v < _min) return 0.0;
	    if (v > _max) return 0.0;
	    countsum += _histogramBuckets[bucketIndex];
	    break;

	 // For non-equality, we need to subtract the count of target bucket from total count
	case NOT_EQUALS:
	    if (v < _min) return 1.0;
	    if (v > _max) return 1.0;
	    countsum = _numTuples - _histogramBuckets[bucketIndex];
	    break;
	}
	
	// If it is a greater/less than or equal operation, we need to add count from the 
	// target bucket as well
	if ((op == Predicate.Op.LESS_THAN_OR_EQ) || (op == Predicate.Op.GREATER_THAN_OR_EQ)){
	    
	    countsum += _histogramBuckets[bucketIndex];
	}

	double selectivity =  countsum/_numTuples;
	return selectivity;
    }
    
    /**
     * @return
     *     the average selectivity of this histogram.
     *     
     *     This is not an indispensable method to implement the basic
     *     join optimization. It may be needed if you want to
     *     implement a more efficient optimization
     * */
    public double avgSelectivity()
    {
	double sum = 0;
        for(int i = 0; i < _histogramBuckets.length; i++){
	    int bucketCount = _histogramBuckets[i];
	    sum += bucketCount*bucketCount/_interval;
	}
	double avgSelectivity = sum/_numTuples; 
        return avgSelectivity;
    }
    
    /**
     * @return A string describing this histogram, for debugging purposes
     */
    public String toString() {

	String description = "(" + Integer.toString(_numTuples) + ") ";
	for(int i = 0; i < _histogramBuckets.length; i++){
	    description = description + "| " + _histogramBuckets[i];
	}
        return description;
    }
   

    /**
     * Initialize the buckets' condition for each range with the right intervals.
     */

    private void initializeBuckets(){

	// If more buckets are requested than the range of the input, then we set every bucket to represent an interval of 1.
	if (_buckets > _range){

	    _interval = 1;
	    _buckets = _range;

	}else{
	    // Else we divide the range evenly amongst the buckets

	    double step = (double) _range / (double) _buckets;
	    step = Math.ceil(step);
	    _interval = (int) step;
	}
	
	_histogramBuckets = new int[_buckets];
	
	// Initializes every bucket with count = 0
	for(int i = 0; i < _buckets; i++){
	    _histogramBuckets[i] = 0;
	    
	}
	
    }
    /**
     * Gets the index of the bucket given value of the input
     * @param v 
     *       Value of the input
     * @return index
     *       Index of bucket
     */
    
    private int getBucketIndex(int v){
	
	if (v == _max) return _buckets-1;
	
	// We are finding how many intervals are v away from the minimum
	int index = (int) Math.floor((v-_min) / _interval);
	return index;
    }

     /**
     *@return 
     *     Total number of tuples in this histogram
     */
    public int totalTuples(){
	return _numTuples;
    }
}

