package simpledb;

import java.io.*;

/**
 * The delete operator. Delete reads tuples from its child operator and removes
 * them from the table they belong to.
 */
public class Delete extends Operator {

    private static final long serialVersionUID = 1L;

    /**
     * Constructor specifying the transaction that this delete belongs to as
     * well as the child to read from.
     * 
     * @param t
     *            The transaction this delete runs in
     * @param child
     *            The child operator from which to read tuples for deletion
     */
    private TransactionId _transid;
    private DbIterator _child;

    /**
     * @param fetched  
     *        To indicate whether fetchNext() has been called. Return null on fetchNext if called more than once
     */
    private boolean fetched;

    public Delete(TransactionId t, DbIterator child) {
        _transid = t;
	_child = child;
	fetched = false;
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
     * Deletes tuples as they are read from the child operator. Deletes are
     * processed via the buffer pool (which can be accessed via the
     * Database.getBufferPool() method.
     * 
     * @return A 1-field tuple containing the number of deleted records.
     * @see Database#getBufferPool
     * @see BufferPool#deleteTuple
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        
        
	if (fetched == true) {
	    return null;
	}

	int count = 0;

	while(_child.hasNext()){
	    Database.getBufferPool().deleteTuple(_transid, _child.next());
	    count++;
	}
	
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
