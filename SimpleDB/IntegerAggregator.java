package simpledb;
import java.util.*;
/**
 * Knows how to compute some aggregate over a set of IntFields.
 */
public class IntegerAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;

    private int _gbfield;
    private Type _gbfieldtype;
    private int _aggregatefield;
    private Op _op;
    
    boolean getNames;
    String f1;
    String f2;

    /**
     *@param dataSet 
     *          contains all the groupBy variables that has been inputed into the IntegerAggregator, it maps each group-by field to its aggregate values
     */
    private HashMap<Field, Integer> dataSet;

    /**
     *@param avgkey 
     *         is only used when _op=AVG, it is to count how many keys there are for each group-by field in the dataset.
     */
    private HashMap<Field, Integer> avgkey;

    /**
     * Aggregate constructor
     * 
     * @param gbfield
     *            the 0-based index of the group-by field in the tuple, or
     *            NO_GROUPING if there is no grouping
     * @param gbfieldtype
     *            the type of the group by field (e.g., Type.INT_TYPE), or null
     *            if there is no grouping
     * @param afield
     *            the 0-based index of the aggregate field in the tuple
     * @param what
     *            the aggregation operator
     */    

    public IntegerAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {

	_gbfield = gbfield;
	_gbfieldtype = gbfieldtype;
	_aggregatefield = afield;
	_op = what;
	dataSet = new HashMap<Field, Integer>();
	avgkey = new HashMap<Field, Integer>();

	getNames = false;
	
    }

    /**
     *Get Current Aggregate Value or Initialize for A Particular Group Key
     *
     *@param key 
     *         The group-by key we intend to get aggregated value for
     */
    
    
    private int getAggregateVal(Field key){ 
	
	if(!dataSet.containsKey(key)) {
	    //Initialize the AggregateValue based on the operation
	    switch(_op){
	    case SUM:
		return 0;
	    case MIN:
		return Integer.MAX_VALUE;
	    case MAX:
		return Integer.MIN_VALUE;
	    case COUNT:
		return 0;
	    case AVG:
		avgkey.put(key,0);
		return 0;
	    }
	    return 0;
	}else{
	    //Return the current AggregateValue for the group-by key
	    return dataSet.get(key);
	}

    }
    
    /**
     *GetHashKey for a Tuple to put into DataSet
     *
     *@param tup
     *        The tuple to get HashKey for the tuple
     *
     *@return If it has a group-by key then return the group-by key, else return null
     */

    private Field getHashKey(Tuple tup){
	if(_gbfield != Aggregator.NO_GROUPING){
	    Field f = tup.getField(_gbfield);
	    return f;
	}else{
	    // If the aggregation has no grouping, we should not seperate the variables using any hashkey at all, so we return null for all, so that all the keys will be aggregated.
	    return null;
	}
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the
     * constructor
     * 
     * @param tup
     *            the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {

	if (!getNames) {
	    TupleDesc tdesc = tup.getTupleDesc();
	    if(_gbfield != Aggregator.NO_GROUPING){
		f1 = tdesc.getFieldName(_gbfield);
	    }
	    f2 = tdesc.getFieldName(_aggregatefield);
	    getNames = true;
	}

	IntField f = (IntField) tup.getField(_aggregatefield);
	assert(f.getType() == Type.INT_TYPE);
	int value = f.getValue();
	Field tupKey = getHashKey(tup);
	int currentAggregate = getAggregateVal(tupKey);

	switch(_op){

	case SUM:
	    currentAggregate += value;
 	    break;

	case MIN:
	    if (currentAggregate > value){
		currentAggregate = value;
	    } 
	    break;

	case MAX:
	    if (currentAggregate < value){
		currentAggregate = value;
	    } 
	    break;

	case AVG:
	    currentAggregate += value;	    
	    int counter = avgkey.get(tupKey);
	    counter++;
	    avgkey.put(tupKey, counter);
	    break;

	case COUNT:
	    currentAggregate++;	    
	    break;  
  
	}
	dataSet.put(tupKey, currentAggregate);
    }

    /**
     *TupleDesc Constructor
     *Constructs the TupleDesc for the output Tuple of IntegerAggregator
     *
     */
    private TupleDesc makeTupleDesc(){
	
	
	if(_gbfield != Aggregator.NO_GROUPING){

	    Type[] typeArray = new Type[2];
	    typeArray[0] = _gbfieldtype;
	    typeArray[1] = Type.INT_TYPE;

	    String[] fieldNames = new String[2];
	    fieldNames[0] = f1;
	    fieldNames[1] = f2;

	    return new TupleDesc(typeArray, fieldNames);
	    //return new TupleDesc(typeArray);

	}else{

	    Type[] typeArray = new Type[1];
	    typeArray[0] = Type.INT_TYPE;

	    String[] fieldName = new String[1];
	    fieldName[0] = f2;

	    return new TupleDesc(typeArray, fieldName);

	    //return new TupleDesc(typeArray);
	}
	
    }

    /**
     * Create a DbIterator over group aggregate results.
     * 
     * @return a DbIterator whose tuples are the pair (groupVal, aggregateVal)
     *         if using group, or a single (aggregateVal) if no grouping. The
     *         aggregateVal is determined by the type of aggregate specified in
     *         the constructor.
     */
    public DbIterator iterator() {

	TupleDesc tupDesc = makeTupleDesc();
	ArrayList<Tuple> result = new ArrayList<Tuple>();

	for(Field f:dataSet.keySet()){

	    Tuple newtup = new Tuple(tupDesc);	    
	    int aggregateVal = dataSet.get(f);

	    if(_op == Aggregator.Op.AVG){
		aggregateVal = aggregateVal / avgkey.get(f);
	    }

	    IntField aggregateField = new IntField(aggregateVal);

	    if (_gbfield != Aggregator.NO_GROUPING){

		newtup.setField(0, f);
		newtup.setField(1, aggregateField);
	 
	    }else{
		
		newtup.setField(0, aggregateField);
	    }

	    result.add(newtup);

        }
	// System.out.println("----------ResultArray-----------------------");
	// System.out.println(_op.toString());
	// for(Tuple t: result){
	//     System.out.println(t.toString());
	// }
	return new TupleIterator(tupDesc, result);
    }

}
