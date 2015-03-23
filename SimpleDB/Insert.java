package simpledb;

import java.io.*;
import java.util.*;
import java.lang.*;

/**
 * Inserts tuples read from the child operator into the tableid specified in the
 * constructor
 */
public class Insert extends Operator {

    private static final long serialVersionUID = 1L;

    private TransactionId _transid;
    private DbIterator _child;
    private int _tableid;
    private TupleDesc childtd;

    /**
     * @param fetched  
     *        To indicate whether fetchNext() has been called. Return null on fetchNext if called more than once
     */
    private boolean fetched;

    /**
     * Constructor.
     * 
     * @param t
     *            The transaction running the insert.
     * @param child
     *            The child operator from which to read tuples to be inserted.
     * @param tableid
     *            The table in which to insert tuples.
     * @throws DbException
     *             if TupleDesc of child differs from table into which we are to
     *             insert.
     */
    
    public Insert(TransactionId t,DbIterator child, int tableid)
            throws DbException {
	
        _transid = t;
	_child = child;
	_tableid = tableid;
	childtd = child.getTupleDesc();
	fetched = false;
	TupleDesc td = Database.getCatalog().getDbFile(tableid).getTupleDesc();
	if (!childtd.equals(td)) {
	    throw new DbException("TupleDesc of child differs from table into which we are to insert.");
	}
    }

    public TupleDesc getTupleDesc() {
	
	Type[] typeArray = new Type[1];
	typeArray[0] = Type.INT_TYPE;
	return new TupleDesc(typeArray);

    }

    public void open() throws DbException, TransactionAbortedException {
        
	_child.open();
	super.open();
    }

    public void close() {
        super.close();
	_child.close();
    }

    public void rewind() throws DbException, TransactionAbortedException {
        _child.rewind();
    }

    /**
     * Inserts tuples read from child into the tableid specified by the
     * constructor. It returns a one field tuple containing the number of
     * inserted records. Inserts should be passed through BufferPool. An
     * instances of BufferPool is available via Database.getBufferPool(). Note
     * that insert DOES NOT need check to see if a particular tuple is a
     * duplicate before inserting it.
     * 
     * @return A 1-field tuple containing the number of inserted records, or
     *         null if called more than once.
     * @see Database#getBufferPool
     * @see BufferPool#insertTuple
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        
	if (fetched == true) return null;
	int count = 0;
	try{
	    while(_child.hasNext()){
		Database.getBufferPool().insertTuple(_transid, _tableid, _child.next());
		count++;
	    }
	}catch(IOException e){
	    e.printStackTrace();
	    System.out.println("IOException in Insert#fetchNext()");	
	}

	// Creates the result tuple which contains the count of the inserts
	Tuple resultTup = new Tuple(getTupleDesc());
	resultTup.setField(0, new IntField(count));
	fetched = true;
        return resultTup;
    }

    @Override
    public DbIterator[] getChildren() {
      
        return new DbIterator[]{ this._child };
    }

    @Override
    public void setChildren(DbIterator[] children) {
        
	this._child = children[0];

    }
}
