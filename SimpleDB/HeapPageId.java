package simpledb;

import java.lang.Integer;

/** Unique identifier for HeapPage objects. */
public class HeapPageId implements PageId {

    /**
     * Constructor. Create a page id structure for a specific page of a
     * specific table.
     *
     * @param tableId The table that is being referenced
     * @param pgNo The page number in that table.
     */

    private int _tableId;
    private int _pgNum;

    public HeapPageId(int tableId, int pgNo) {
        _tableId = tableId;
	_pgNum = pgNo;
    }

    /** @return the table associated with this PageId */
    public int getTableId() {
       
        return _tableId;
    }

    /**
     * @return the page number in the table getTableId() associated with
     *   this PageId
     */
    public int pageNumber() {
        
        return _pgNum;
    }

    /**
     * @return a hash code for this page, represented by the concatenation of
     *   the table number and the page number (needed if a PageId is used as a
     *   key in a hash table in the BufferPool, for example.)
     * @see BufferPool
     */
    public int hashCode() {
	int hashNum = (_tableId * 213794 + _pgNum*5684)%15600047;
	// Following line throws NumberFormatException in testIteratorBasic.java
	// int hashNum = Integer.parseInt(Integer.toString(_tableId) + Integer.toString(_pgNum));
        return hashNum;
        
    }

    /**
     * Compares one PageId to another.
     *
     * @param o The object to compare against (must be a PageId)
     * @return true if the objects are equal (e.g., page numbers and table
     *   ids are the same)
     */
    public boolean equals(Object o) {
        try{
	    HeapPageId other = (HeapPageId) o;
	    return ((this._tableId == other.getTableId()) && (this._pgNum == other.pageNumber()));
	}catch(Exception e){
	    return false;
	}
    }

    /**
     *  Return a representation of this object as an array of
     *  integers, for writing to disk.  Size of returned array must contain
     *  number of integers that corresponds to number of args to one of the
     *  constructors.
     */
    public int[] serialize() {
        int data[] = new int[2];

        data[0] = getTableId();
        data[1] = pageNumber();

        return data;
    }

}
