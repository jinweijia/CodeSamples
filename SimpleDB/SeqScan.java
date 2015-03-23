package simpledb;

import java.util.*;

/**
 * SeqScan is an implementation of a sequential scan access method that reads
 * each tuple of a table in no particular order (e.g., as they are laid out on
 * disk).
 */
public class SeqScan implements DbIterator {

    private static final long serialVersionUID = 1L;

    /**
     * Creates a sequential scan over the specified table as a part of the
     * specified transaction.
     * 
     * @param tid
     *            The transaction this scan is running as a part of.
     * @param tableid
     *            the table to scan.
     * @param tableAlias
     *            the alias of this table (needed by the parser); the returned
     *            tupleDesc should have fields with name tableAlias.fieldName
     *            (note: this class is not responsible for handling a case where
     *            tableAlias or fieldName are null. It shouldn't crash if they
     *            are, but the resulting name can be null.fieldName,
     *            tableAlias.null, or null.null).
     */
    private TransactionId _tid;
    private int _tableId;
    private String _tableAlias;
    private DbFile _dbFile;
    private DbFileIterator _dbIterator;

    public SeqScan(TransactionId tid, int tableid, String tableAlias) {
        _tid = tid;
	_tableId = tableid;
	if (tableAlias == null) {
	    _tableAlias = "null";
	} else {
	    _tableAlias = tableAlias;
	}
	_dbFile = Database.getCatalog().getDbFile(_tableId);
	
    }

    /**
     * @return
     *       return the table name of the table the operator scans. This should
     *       be the actual name of the table in the catalog of the database
     * */
    public String getTableName() {
        
	return Database.getCatalog().getTableName(_tableId);
    }
    
    /**
     * @return Return the alias of the table this operator scans. 
     * */
    public String getAlias()
    {
        return _tableAlias;
    }

    /**
     * Reset the tableid, and tableAlias of this operator.
     * @param tableid
     *            the table to scan.
     * @param tableAlias
     *            the alias of this table (needed by the parser); the returned
     *            tupleDesc should have fields with name tableAlias.fieldName
     *            (note: this class is not responsible for handling a case where
     *            tableAlias or fieldName are null. It shouldn't crash if they
     *            are, but the resulting name can be null.fieldName,
     *            tableAlias.null, or null.null).
     */
    public void reset(int tableid, String tableAlias) {
	_tableId = tableid;
	_tableAlias = tableAlias;
    }

    public SeqScan(TransactionId tid, int tableid) {
        this(tid, tableid, Database.getCatalog().getTableName(tableid));
    }

    public void open() throws DbException, TransactionAbortedException {
	_dbIterator = ((HeapFile)_dbFile).iterator(_tid);
        _dbIterator.open();
    }

    /**
     * Returns the TupleDesc with field names from the underlying HeapFile,
     * prefixed with the tableAlias string from the constructor. This prefix
     * becomes useful when joining tables containing a field(s) with the same
     * name.
     * 
     * @return the TupleDesc with field names from the underlying HeapFile,
     *         prefixed with the tableAlias string from the constructor.
     */
    public TupleDesc getTupleDesc() {
	TupleDesc td = _dbFile.getTupleDesc();
	int tdLen = td.numFields();
	Type[] types = new Type[tdLen];
	String[] names = new String[tdLen];
	// For each item in td, get field Type and field Name. Append _tableAlias to front of name
	// and return a new TupleDesc object.
	for (int i=0; i<tdLen; i++) {
	    types[i] = td.getFieldType(i);
	    String n = td.getFieldName(i);
	    if (n==null) n = "null";
	    names[i] = _tableAlias + "." + n;
	}
	return new TupleDesc(types, names);

   }

    public boolean hasNext() throws TransactionAbortedException, DbException {   
        return _dbIterator.hasNext();
    }

    public Tuple next() throws NoSuchElementException,
            TransactionAbortedException, DbException {
        
        return _dbIterator.next();
    }

    public void close() {
        _dbIterator.close();
    }

    public void rewind() throws DbException, NoSuchElementException,
            TransactionAbortedException {
        _dbIterator.rewind();
    }
}
