package simpledb;

import java.util.*;

/**
 * Filter is an operator that implements a relational select.
 */
public class Filter extends Operator {

    private static final long serialVersionUID = 1L;

    /**
     * Constructor accepts a predicate to apply and a child operator to read
     * tuples to filter from.
     * 
     * @param p
     *            The predicate to filter tuples with
     * @param child
     *            The child operator
     */
    Predicate _predicate;
    DbIterator _dbIterator;

    public Filter(Predicate p, DbIterator child) {
	_predicate = p;
	_dbIterator = child;
    }

    public Predicate getPredicate() {
       
        return _predicate;
    }

    public TupleDesc getTupleDesc() {
        
        return _dbIterator.getTupleDesc();
    }

    public void open() throws DbException, NoSuchElementException,
            TransactionAbortedException {
        _dbIterator.open();
	super.open();
    }

    public void close() {
	super.close();
        _dbIterator.close();
    }

    public void rewind() throws DbException, TransactionAbortedException {
       
	_dbIterator.rewind();
	
    }

    /**
     * AbstractDbIterator.readNext implementation. Iterates over tuples from the
     * child operator, applying the predicate to them and returning those that
     * pass the predicate (i.e. for which the Predicate.filter() returns true.)
     * 
     * @return The next tuple that passes the filter, or null if there are no
     *         more tuples
     * @see Predicate#filter
     */
    protected Tuple fetchNext() throws NoSuchElementException,
            TransactionAbortedException, DbException {

	    while(_dbIterator.hasNext()){
		Tuple nextTup = _dbIterator.next();
		if(_predicate.filter(nextTup)){
		    return nextTup;
		}
	    }
	    return null;

    }

    @Override
    public DbIterator[] getChildren() {
        return new DbIterator[]{ this._dbIterator };
    }

    @Override
    public void setChildren(DbIterator[] children) {
        if (this._dbIterator != children[0]){
	    this._dbIterator = children[0];
	}
    }

}
