package simpledb;

import java.io.Serializable;
import java.util.*;
// import java.util.Iterator;
// import java.util.ArrayList;

/**
 * Tuple maintains information about the contents of a tuple. Tuples have a
 * specified schema specified by a TupleDesc object and contain Field objects
 * with the data for each field.
 */
public class Tuple implements Serializable {

    private static final long serialVersionUID = 1L;

    private TupleDesc schema = null;
    private RecordId recordID = null;

    // An arraylist of Field objects 
    private ArrayList<Field> fieldArray = null;

    /**
     * Create a new tuple with the specified schema (type).
     * 
     * @param td
     *            the schema of this tuple. It must be a valid TupleDesc
     *            instance with at least one field.
     */
    public Tuple(TupleDesc td) {
        schema = td;
	fieldArray = new ArrayList<Field>(td.numFields());
	for (int i=0; i<td.numFields(); i++){
	    fieldArray.add(null);
	}
    }

    /**
     * @return The TupleDesc representing the schema of this tuple.
     */
    public TupleDesc getTupleDesc() {
      
        return schema;
    }

    /**
     * @return The RecordId representing the location of this tuple on disk. May
     *         be null.
     */
    public RecordId getRecordId() {
        
        return recordID;
    }

    /**
     * Set the RecordId information for this tuple.
     * 
     * @param rid
     *            the new RecordId for this tuple.
     */
    public void setRecordId(RecordId rid) {
        recordID = rid;
    }

    /**
     * Change the value of the ith field of this tuple.
     * 
     * @param i
     *            index of the field to change. It must be a valid index.
     * @param f
     *            new value for the field.
     */
    public void setField(int i, Field f) {
	if(schema.getFieldType(i) == f.getType()){
	    fieldArray.set(i, f);
	}else{
	    throw new RuntimeException("Field of wrong type");
	}
    }

    /**
     * @return the value of the ith field, or null if it has not been set.
     * 
     * @param i
     *            field index to return. Must be a valid index.
     */
    public Field getField(int i) {
	try{
	    return fieldArray.get(i);
	}catch(NullPointerException e){
	    return null;
	}        
    }

    /**
     * Returns the contents of this Tuple as a string. Note that to pass the
     * system tests, the format needs to be as follows:
     * 
     * column1\tcolumn2\tcolumn3\t...\tcolumnN\n
     * 
     * where \t is any whitespace, except newline, and \n is a newline
     */
    public String toString() {
        
	String description = "";
	for(int i = 0; i < fieldArray.size(); i++){
	    Field item = fieldArray.get(i);
	    description = description.concat(item.toString()).concat(" ");
	    
	}       
        return description+"\n";
        
    }
    
    /**
     * @return
     *        An iterator which iterates over all the fields of this tuple
     * */
    public Iterator<Field> fields()
    {
        
        return fieldArray.iterator();
    }

    public int getSize(){
	return schema.getSize();
    }
}
