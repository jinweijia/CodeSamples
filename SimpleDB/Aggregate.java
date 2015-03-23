package simpledb;

import java.util.*;

/**
 * The Aggregation operator that computes an aggregate (e.g., sum, avg, max,
 * min). Note that we only support aggregates over a single column, grouped by a
 * single column.
 */
public class Aggregate extends Operator {

    private static final long serialVersionUID = 1L;

    DbIterator _child;
    TupleDesc tupDesc;
    int _afield;
    int _gfield;
    Aggregator.Op _op;
    Aggregator _aggregator;
    DbIterator _aggregateIterator;

    /**
     * Constructor.
     * 
     * Implementation hint: depending on the type of afield, you will want to
     * construct an {@link IntAggregator} or {@link StringAggregator} to help
     * you with your implementation of readNext().
     * 
     * 
     * @param child
     *            The DbIterator that is feeding us tuples.
     * @param afield
     *            The column over which we are computing an aggregate.
     * @param gfield
     *            The column over which we are grouping the result, or -1 if
     *            there is no grouping
     * @param aop
     *            The aggregation operator to use
     */   

    public Aggregate(DbIterator child, int afield, int gfield, Aggregator.Op aop) {
	_child = child;
	tupDesc = _child.getTupleDesc();
	_afield = afield;
	_gfield = gfield;
	_op = aop;
	makeAggregator();
    }

    private void makeAggregator(){
	
	
	Type afieldType = _child.getTupleDesc().getFieldType(_afield);
	Type gbfieldType = null;
	
	if(_gfield != Aggregator.NO_GROUPING){
	    gbfieldType = _child.getTupleDesc().getFieldType(_gfield);
	}

	if(afieldType == Type.INT_TYPE){
	    _aggregator = new IntegerAggregator(_gfield, gbfieldType, _afield, _op);
	}else if(afieldType == Type.STRING_TYPE){
	    _aggregator = new StringAggregator(_gfield, gbfieldType, _afield, _op);
	}
 	
    }

    /**
     * @return If this aggregate is accompanied by a groupby, return the groupby
     *         field index in the <b>INPUT</b> tuples. If not, return
     *         {@link simpledb.Aggregator#NO_GROUPING}
     * */
    public int groupField() {
      
	return _gfield;
    }

    /**
     * @return If this aggregate is accompanied by a group by, return the name
     *         of the groupby field in the <b>OUTPUT</b> tuples If not, return
     *         null;
     * */
    public String groupFieldName() {
      
	if(_gfield == Aggregator.NO_GROUPING){
	    return null;
	}
	
	return _child.getTupleDesc().getFieldName(_gfield);
    }

    /**
     * @return the aggregate field
     * */
    public int aggregateField() {
      
	return _afield;
    }

    /**
     * @return return the name of the aggregate field in the <b>OUTPUT</b>
     *         tuples
     * */
    public String aggregateFieldName() {
      
	
	return _child.getTupleDesc().getFieldName(_afield);
    }

    /**
     * @return return the aggregate operator
     * */
    public Aggregator.Op aggregateOp() {
       
	return _op;
    }

    public static String nameOfAggregatorOp(Aggregator.Op aop) {
	return aop.toString();
    }

    public void open() throws NoSuchElementException, DbException,
	    TransactionAbortedException {
	
	_child.open();	
	while(_child.hasNext()){
	    _aggregator.mergeTupleIntoGroup(_child.next());
	}
	_aggregateIterator = _aggregator.iterator();
	_aggregateIterator.open();
	super.open();

    }

    /**
     * Returns the next tuple. If there is a group by field, then the first
     * field is the field by which we are grouping, and the second field is the
     * result of computing the aggregate, If there is no group by field, then
     * the result tuple should contain one field representing the result of the
     * aggregate. Should return null if there are no more tuples.
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
	// some code goes here
	if(_aggregateIterator.hasNext()){
	    return _aggregateIterator.next();  
	}
	return null;
    }

    public void rewind() throws DbException, TransactionAbortedException {
     
	_aggregateIterator.rewind();
    }

    /**
     * Returns the TupleDesc of this Aggregate. If there is no group by field,
     * this will have one field - the aggregate column. If there is a groupby
     * field, the first field will be the group by field, and the second will be
     * the aggregate value column.
     * 
     * The name of an aggregate column should be informative. For example:
     * "aggName(aop) (child_td.getFieldName(afield))" where aop and afield are
     * given in the constructor, and child_td is the TupleDesc of the child
     * iterator.
     */
    public TupleDesc getTupleDesc() {
	
	//return _aggregator.iterator().getTupleDesc();

	if(_gfield != Aggregator.NO_GROUPING){

	    Type[] typeArray = new Type[2];
	    typeArray[0] =  _child.getTupleDesc().getFieldType(_gfield);
	    typeArray[1] = Type.INT_TYPE;

	    String[] fieldNames = new String[2];
	    fieldNames[0] = groupFieldName();
	    fieldNames[1] = "" + nameOfAggregatorOp(_op) + " (" + aggregateFieldName() + ")";

	    return new TupleDesc(typeArray, fieldNames);	    

	}else{

	    Type[] typeArray = new Type[1];
	    typeArray[0] = Type.INT_TYPE;

	    String[] fieldName = new String[1];
	    fieldName[0] = "" + nameOfAggregatorOp(_op) + " (" + aggregateFieldName() + ")";

	    return new TupleDesc(typeArray, fieldName);
	    
	}
       
    }

    public void close() {
   
	super.close();
	_aggregateIterator.close();
	
    }

    @Override
    public DbIterator[] getChildren() {
	// some code goes here
	return new DbIterator[]{ this._child };
    }

    @Override
    public void setChildren(DbIterator[] children) {
	// some code goes here
	if (this._child != children[0]){
	    this._child = children[0];
	}
    }
  

}
