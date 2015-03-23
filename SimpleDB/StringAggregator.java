package simpledb;
import java.util.*;

/**
 * Knows how to compute some aggregate over a set of StringFields.
 */
public class StringAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;

    int _gbfield;
    Type _gbfieldtype;
    int _aggregatefield;
    Op _op;

    boolean getNames;
    String f1;
    String f2;

    HashMap<Field, Integer> dataSet;

    /**
     * Aggregate constructor
     * @param gbfield the 0-based index of the group-by field in the tuple, or NO_GROUPING if there is no grouping
     * @param gbfieldtype the type of the group by field (e.g., Type.INT_TYPE), or null if there is no grouping
     * @param afield the 0-based index of the aggregate field in the tuple
     * @param what aggregation operator to use -- only supports COUNT
     * @throws IllegalArgumentException if what != COUNT
     */    
    public StringAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        // some code goes here

	_gbfield = gbfield;
	_gbfieldtype = gbfieldtype;
	_aggregatefield = afield;
	if (what != Aggregator.Op.COUNT) throw new IllegalArgumentException();
	_op = what;
	dataSet = new HashMap<Field, Integer>();

	getNames = false;
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the constructor
     * @param tup the Tuple containing an aggregate field and a group-by field
     */

    private int getAggregateVal(Field key){
	 
	if(!dataSet.containsKey(key)){    
	    return 0;
	}else{
	    return dataSet.get(key);
	}

    }

    // private int getGroupByKey(Tuple tup){
    // 	Field f = tup.getField(_gbfield);
    // 	Type t = f.getType();
    // 	int tupGbfield = ((IntField) f).getValue();
    // 	return tupGbfield;

    // }

    private Field getHashKey(Tuple tup){
	Field f = tup.getField(_gbfield);
	return f;
    }

    public void mergeTupleIntoGroup(Tuple tup) {

	if (!getNames) {
	    TupleDesc tdesc = tup.getTupleDesc();
	    if(_gbfield != Aggregator.NO_GROUPING){
		f1 = tdesc.getFieldName(_gbfield);
	    }
	    f2 = tdesc.getFieldName(_aggregatefield);
	    getNames = true;
	}

	StringField f = (StringField) tup.getField(_aggregatefield);
	assert(f.getType() == Type.STRING_TYPE);
	String tupAfield = f.getValue();
	Field tupKey = getHashKey(tup);
	int currentAggregate = getAggregateVal(tupKey);
	
	currentAggregate++;
	
	dataSet.put(tupKey, currentAggregate);
    }

    private TupleDesc makeTupleDesc(){

	
	if(_gbfield != Aggregator.NO_GROUPING){
	    Type[] typeArray = new Type[2];
	    typeArray[0] = _gbfieldtype;
	    typeArray[1] = Type.INT_TYPE;

	    String[] fieldNames = new String[2];
	    fieldNames[0] = f1;
	    fieldNames[1] = f2;

	    return new TupleDesc(typeArray, fieldNames);

	}else{

	    Type[] typeArray = new Type[1];
	    typeArray[0] = Type.INT_TYPE;

	    String[] fieldName = new String[1];
	    fieldName[0] = f2;

	    return new TupleDesc(typeArray, fieldName);
	}
	
    }
   

    /**
     * Create a DbIterator over group aggregate results.
     *
     * @return a DbIterator whose tuples are the pair (groupVal,
     *   aggregateVal) if using group, or a single (aggregateVal) if no
     *   grouping. The aggregateVal is determined by the type of
     *   aggregate specified in the constructor.
     */
    public DbIterator iterator() {
        // some code goes here

	TupleDesc tupDesc = makeTupleDesc();
	ArrayList<Tuple> result = new ArrayList<Tuple>();

	for(Field f:dataSet.keySet()){

	    Tuple newtup = new Tuple(tupDesc);	    
	    int aggregateVal = dataSet.get(f);
	    IntField aggregateField = new IntField(aggregateVal);

	    if (_gbfield != Aggregator.NO_GROUPING){
		newtup.setField(0, f);
		newtup.setField(1, aggregateField);
	    }else{
		newtup.setField(0, aggregateField);
	    }
	    result.add(newtup);
        }
	return new TupleIterator(tupDesc, result);
    }

}
