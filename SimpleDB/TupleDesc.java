package simpledb;

import java.io.Serializable;
import java.util.*;

/**
 * TupleDesc describes the schema of a tuple.
 */
public class TupleDesc implements Serializable {

    /**
     * A help class to facilitate organizing the information of each field
     * */
    /**
     * An arraylist of fields
     */

    private ArrayList<TDItem> fieldArray = null;
    /**
     * An array of type variables
     */
    private Type[] typeArray = null;

    public static class TDItem implements Serializable {

        private static final long serialVersionUID = 1L;

        /**
         * The type of the field
         * */
        Type fieldType;
        
        /**
         * The name of the field
         * */
        String fieldName;

        public TDItem(Type t, String n) {
            this.fieldName = n;
            this.fieldType = t;
        }

        public String toString() {
            return fieldName + "(" + fieldType + ")";
        }
    }

    /**
     * @return
     *        An iterator which iterates over all the field TDItems
     *        that are included in this TupleDesc
     * */
    public Iterator<TDItem> iterator() {
	Iterator<TDItem> iter = fieldArray.iterator();
        return iter;
    }

    private static final long serialVersionUID = 1L;

    /**
     * Create a new TupleDesc with typeAr.length fields with fields of the
     * specified types, with associated named fields.
     * 
     * @param typeAr
     *            array specifying the number of and types of fields in this
     *            TupleDesc. It must contain at least one entry.
     * @param fieldAr
     *            array specifying the names of the fields. Note that names may
     *            be null.
     */
    public TupleDesc(Type[] typeAr, String[] fieldAr) {
	
	fieldArray = new ArrayList<TDItem>(typeAr.length);
	typeArray = typeAr;
	for(int i = 0; i < typeAr.length; i++){
	    
	    fieldArray.add(i, new TDItem(typeAr[i], fieldAr[i]));
	}
    }

    /**
     * Constructor. Create a new tuple desc with typeAr.length fields with
     * fields of the specified types, with anonymous (unnamed) fields.
     * 
     * @param typeAr
     *            array specifying the number of and types of fields in this
     *            TupleDesc. It must contain at least one entry.
     */
    public TupleDesc(Type[] typeAr) {
	// fieldArray = new TDItem[typeAr.length];
	fieldArray = new ArrayList<TDItem>(typeAr.length);
	typeArray = typeAr;
	for(int i = 0; i < typeAr.length; i++){
	    
	    fieldArray.add(i, new TDItem(typeAr[i], ""));
	}
    }

    /**
     * @return the number of fields in this TupleDesc
     */
    public int numFields() {
       
        return fieldArray.size();
    }

    /**
     * Gets the (possibly null) field name of the ith field of this TupleDesc.
     * 
     * @param i
     *            index of the field name to return. It must be a valid index.
     * @return the name of the ith field
     * @throws NoSuchElementException
     *             if i is not a valid field reference.
     */
    public String getFieldName(int i) throws NoSuchElementException {
	try{
	    TDItem elem = fieldArray.get(i);
	    return elem.fieldName;
	}catch(Exception e){
	    throw new NoSuchElementException();
	}
	
    }


    /**
     * Gets the type of the ith field of this TupleDesc.
     * 
     * @param i
     *            The index of the field to get the type of. It must be a valid
     *            index.
     * @return the type of the ith field
     * @throws NoSuchElementException
     *             if i is not a valid field reference.
     */
    public Type getFieldType(int i) throws NoSuchElementException {
	if (i >= fieldArray.size() || i<0) {
	    throw new NoSuchElementException();
	}
	TDItem elem = fieldArray.get(i);	           
	Type type = elem.fieldType;
	return type;
	
    }

    /**
     * Find the index of the field with a given name.
     * 
     * @param name
     *            name of the field.
     * @return the index of the field that is first to have the given name.
     * @throws NoSuchElementException
     *             if no field with a matching name is found.
     */
    public int fieldNameToIndex(String name) throws NoSuchElementException {
       
	if((fieldArray == null) || (name == null)){
	    throw new NoSuchElementException("");
	}

	for(int i = 0; i < fieldArray.size(); i++){
	    
	    if(this.getFieldName(i) == null){
		continue;
	    }
	    if(this.getFieldName(i).equals(name)){
		return i;
	    }
	}

	throw new NoSuchElementException("No field with name: " + name);
       
    }

    /**
     * @return The size (in bytes) of tuples corresponding to this TupleDesc.
     *         Note that tuples from a given TupleDesc are of a fixed size.
     */
    public int getSize() {
	int size = 0;
	
	if(typeArray == null){
	    return 0;
	}

	for(int i = 0; i < typeArray.length; i++){
	    if(typeArray[i] == null){
		continue;
	    }
	    size += typeArray[i].getLen();
	}

       
	return size;
    }

    private TDItem getTDItemAtIndex(int i){
	return fieldArray.get(i);
    }

    /**
     * Merge two TupleDescs into one, with td1.numFields + td2.numFields fields,
     * with the first td1.numFields coming from td1 and the remaining from td2.
     * 
     * @param td1
     *            The TupleDesc with the first fields of the new TupleDesc
     * @param td2
     *            The TupleDesc with the last fields of the TupleDesc
     * @return the new TupleDesc
     */
    public static TupleDesc merge(TupleDesc td1, TupleDesc td2) {

	assert(td1 != null);
	assert(td2 != null);

        int newlength = td1.numFields() + td2.numFields();
	int shift = td1.numFields();

	String[] newFieldArray = new String[newlength];
	Type[] newTypeArray = new Type[newlength];

	for(int i = 0; i < td1.numFields(); i++){
	    newFieldArray[i] = td1.getFieldName(i);
	    newTypeArray[i] = td1.getFieldType(i);
	}

	for(int i = 0; i < td2.numFields(); i++){
	    newFieldArray[i+shift] = td2.getFieldName(i);
	    newTypeArray[i+shift] = td2.getFieldType(i);
	}
        return new TupleDesc(newTypeArray, newFieldArray);
    }

    /**
     * Compares the specified object with this TupleDesc for equality. Two
     * TupleDescs are considered equal if they are the same size and if the n-th
     * type in this TupleDesc is equal to the n-th type in td.
     * 
     * @param o
     *            the Object to be compared for equality with this TupleDesc.
     * @return true if the object is equal to this TupleDesc.
     */
    public boolean equals(Object o) {
	
	
	assert(typeArray != null);
	try{
	    TupleDesc other = (TupleDesc) o;
	    if(other.getSize() != this.getSize()){
		return false;
	    }
	    for(int i = 0; i < typeArray.length; i++){
		if(!(this.getFieldType(i) == other.getFieldType(i))){
		    return false;
		}

	    }
	    return true;
	}catch(ClassCastException e){
	    System.out.println("Not a TupleDesc");
	    return false;
	}catch(NullPointerException e){
	    System.out.println("Null Object");
	    return false;
	}
	    
    }

    public int hashCode() {
        // If you want to use TupleDesc as keys for HashMap, implement this so
        // that equal objects have equals hashCode() results

        throw new UnsupportedOperationException("unimplemented");
    }

    /**
     * Returns a String describing this descriptor. It should be of the form
     * "fieldType[0](fieldName[0]), ..., fieldType[M](fieldName[M])", although
     * the exact format does not matter.
     * 
     * @return String describing this descriptor.
     */
    public String toString() {
	String description = "";
	for(int i = 0; i < fieldArray.size(); i++){
	    TDItem item = fieldArray.get(i);
	    description = description.concat(item.toString()).concat(", ");
	    
	}
       
        return description;
    }
}
