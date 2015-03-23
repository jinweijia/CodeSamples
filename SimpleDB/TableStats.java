package simpledb;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * TableStats represents statistics (e.g., histograms) about base tables in a
 * query. 
 * 
 * This class is not needed in implementing proj1 and proj2.
 */
public class TableStats {

    private static final ConcurrentHashMap<String, TableStats> statsMap = new ConcurrentHashMap<String, TableStats>();

    static final int IOCOSTPERPAGE = 1000;

    public static TableStats getTableStats(String tablename) {
        return statsMap.get(tablename);
    }

    public static void setTableStats(String tablename, TableStats stats) {
        statsMap.put(tablename, stats);
    }
    
    public static void setStatsMap(HashMap<String,TableStats> s)
    {
        try {
            java.lang.reflect.Field statsMapF = TableStats.class.getDeclaredField("statsMap");
            statsMapF.setAccessible(true);
            statsMapF.set(null, s);
        } catch (NoSuchFieldException e) {
            e.printStackTrace();
        } catch (SecurityException e) {
            e.printStackTrace();
        } catch (IllegalArgumentException e) {
            e.printStackTrace();
        } catch (IllegalAccessException e) {
            e.printStackTrace();
        }

    }

    public static Map<String, TableStats> getStatsMap() {
        return statsMap;
    }

    public static void computeStatistics() {
        Iterator<Integer> tableIt = Database.getCatalog().tableIdIterator();

        System.out.println("Computing table stats.");
        while (tableIt.hasNext()) {
            int tableid = tableIt.next();
            TableStats s = new TableStats(tableid, IOCOSTPERPAGE);
            setTableStats(Database.getCatalog().getTableName(tableid), s);
        }
        System.out.println("Done.");
    }

    /**
     * Number of bins for the histogram. Feel free to increase this value over
     * 100, though our tests assume that you have at least 100 bins in your
     * histograms.
     */
    static final int NUM_HIST_BINS = 100;

    /**
     * Create a new TableStats object, that keeps track of statistics on each
     * column of a table
     * 
     * @param tableid
     *            The table over which to compute statistics
     * @param ioCostPerPage
     *            The cost per page of IO. This doesn't differentiate between
     *            sequential-scan IO and disk seeks.
     */

    int _tableid;
    int _ioCostPerPage;

    /* Heap file containing the table */
    HeapFile _tableFile;
    
    /* Total number of tuples in this table */
    int _totalTuples;

    /* Array of histograms, one for each field*/
    Object[] _histograms;

    public TableStats(int tableid, int ioCostPerPage) {
        // For this function, you'll have to get the
        // DbFile for the table in question,
        // then scan through its tuples and calculate
        // the values that you need.
        // You should try to do this reasonably efficiently, but you don't
        // necessarily have to (for example) do everything
        // in a single scan of the table.

        _tableid = tableid;
	_ioCostPerPage = ioCostPerPage;
	_tableFile = (HeapFile) Database.getCatalog().getDbFile(tableid);
	
	generateHistograms();
    }

    /**
     * Estimates the cost of sequentially scanning the file, given that the cost
     * to read a page is costPerPageIO. You can assume that there are no seeks
     * and that no pages are in the buffer pool.
     * 
     * Also, assume that your hard drive can only read entire pages at once, so
     * if the last page of the table only has one tuple on it, it's just as
     * expensive to read as a full page. (Most real hard drives can't
     * efficiently address regions smaller than a page at a time.)
     * 
     * @return The estimated cost of scanning the table.
     */
    public double estimateScanCost() {

        int numPage = _tableFile.numPages();
      	int cost = numPage * _ioCostPerPage;
        return cost;
    }

    /**
     * This method returns the number of tuples in the relation, given that a
     * predicate with selectivity selectivityFactor is applied.
     * 
     * @param selectivityFactor
     *            The selectivity of any predicates over the table
     * @return The estimated cardinality of the scan with the specified
     *         selectivityFactor
     */
    public int estimateTableCardinality(double selectivityFactor) {
        int field = 0;
	int count = 0;
	TupleDesc tupDesc = _tableFile.getTupleDesc();
	Type type = tupDesc.getFieldType(field);
	
	// We can call estimateCardinality on the histograms we initialized and populated
	//   for the first field (index 0) to get the cadinality.
	switch(type){

	case INT_TYPE:
	    IntHistogram intHistogram = (IntHistogram) _histograms[field];
	    count = intHistogram.totalTuples();
	    break;

	case STRING_TYPE:
	    StringHistogram strHistogram = (StringHistogram) _histograms[field];
	    count = strHistogram.totalTuples();
	    break;
	}
	int estimatedCardinality = (int) Math.floor(count*selectivityFactor);
        return estimatedCardinality;
    }

    /**
     * The average selectivity of the field under op.
     * @param field
     *        the index of the field
     * @param op
     *        the operator in the predicate
     * The semantic of the method is that, given the table, and then given a
     * tuple, of which we do not know the value of the field, return the
     * expected selectivity. You may estimate this value from the histograms.
     * */
    public double avgSelectivity(int field, Predicate.Op op) {
        
	TupleDesc tupDesc = _tableFile.getTupleDesc();
	Type type = tupDesc.getFieldType(field);

	// We can call avgSelectivity on the histograms we initialized and populated 
	//   for the target field to get the average selectivity.
	switch(type){
	case INT_TYPE:
	    IntHistogram intHistogram = (IntHistogram) _histograms[field];
	    return intHistogram.avgSelectivity();
	    
	case STRING_TYPE:
	    StringHistogram strHistogram = (StringHistogram) _histograms[field];
	    return strHistogram.avgSelectivity();
	}
        return 1.0;
    }

    /**
     * Estimate the selectivity of predicate <tt>field op constant</tt> on the
     * table.
     * 
     * @param field
     *            The field over which the predicate ranges
     * @param op
     *            The logical operation in the predicate
     * @param constant
     *            The value against which the field is compared
     * @return The estimated selectivity (fraction of tuples that satisfy) the
     *         predicate
     */
    public double estimateSelectivity(int field, Predicate.Op op, Field constant) {
        
	switch(constant.getType()){
	
	// We can just call estimateSelectivity on the histograms we initialized and populated 
	//   for each field to get the selectivity.

	case INT_TYPE:
	    IntHistogram intHistogram = (IntHistogram) _histograms[field];
	    int otherInt = ((IntField) constant).getValue();
	    return intHistogram.estimateSelectivity(op, otherInt);
	    
	case STRING_TYPE:
	    StringHistogram strHistogram = (StringHistogram) _histograms[field];
	    String otherStr = ((StringField) constant).getValue();
	    return strHistogram.estimateSelectivity(op, otherStr);
	    
	}
	
	// This is an invalid case
        return -1;
    }

    /**
     * return the total number of tuples in this table
     * */
    public int totalTuples() {
       
        return _totalTuples;
    }

    /**
     * Creates a histogram for each field in the table.
     * We can later just call functions on the histograms for the field to estimate selectivity
     *   and cardinality etc from the histograms we created
     */

    private void generateHistograms(){

	TupleDesc tupDesc = _tableFile.getTupleDesc();
	_histograms = new Object[tupDesc.numFields()];
	
	// Create a histogram and populate that histogram for each field
	for(int i = 0; i < tupDesc.numFields(); i++){

	    initializeHistogram(i);
	    populateHistogram(i);
	}
    }

    /**
     * Initializes the histogram for every field in the table.
     * It first scans the entire table once for the max and 
     *   min value to be inputed into the histogram. 
     * Then create either a IntHistogram or StringHistogram based on the type
     *   of the field, with the max and min value obtained in the initial scan
     * Each histogram is stored in the index corresponding to its field index
     *
     * @param fieldIndex
     *        The index of the field in TupleDesc
     */
    private void initializeHistogram(int fieldIndex){
	
	DbFileIterator it = _tableFile.iterator(new TransactionId());
	try{
	    it.open();
	    if(it.hasNext()){

		Tuple firstTup = it.next();
		Field firstField =  firstTup.getField(fieldIndex);
		int max = Integer.MIN_VALUE;
		int min = Integer.MAX_VALUE;
	
		// Initializes the max and min of the table to the value of the first tuple
		switch(firstField.getType()){
		case INT_TYPE:
		    max = ((IntField) firstTup.getField(fieldIndex)).getValue();
		    min = ((IntField) firstTup.getField(fieldIndex)).getValue();
		    break;
		   
		}

		Type type = firstTup.getField(fieldIndex).getType();
		int counter = 1;
	    
		// Scan through the entire table to find the correct maximum and minimum for the field
		while(it.hasNext()){
		
		    Tuple t = it.next();
		    Field f = t.getField(fieldIndex);
		    switch(firstField.getType()){
		    case INT_TYPE:
			int val = ((IntField) f).getValue();
			if (val > max) max = val;
			if (val < min) min = val;
			counter++;
			
		    }
		   
		}
       
		// Sets the total number of tuple in the tablefile
		_totalTuples = counter;
		
		// Depending on the type of field, we create either a inthistogram or stringhistogram
		//   and store them in array of histograms
		switch(type){

		case INT_TYPE:

		    IntHistogram histogram = new IntHistogram(NUM_HIST_BINS, min, max);
		    _histograms[fieldIndex] = histogram;
		    break;

		case STRING_TYPE:
		    StringHistogram strHistogram = new StringHistogram(NUM_HIST_BINS);
		    _histograms[fieldIndex] = strHistogram;
		    break;
		}
	    }
	    
	    it.close();

	}catch(Exception e){
	    e.printStackTrace();
	    System.out.println("Error in initializing histogram for fieldIndex: " + fieldIndex);
	}
	
    }
    /**
     * Populates the histogram with actual values for each field
     * It inserts the value of field with a particular index, tuple by tuple.
     *
     * @param fieldIndex
     *        The index of the field in TupleDesc
     */
    private void populateHistogram(int fieldIndex){
	
	DbFileIterator it = _tableFile.iterator(new TransactionId());

	try{
	    it.open();
	    
	    // Scan through the entire table then insert the field value of each tuple into histogram
	    while(it.hasNext()){
	
		Tuple tup = it.next();
		Field field = tup.getField(fieldIndex);

		switch(field.getType()){
		case INT_TYPE:
		    int intValue = ((IntField) field).getValue();
		    IntHistogram intHistogram = (IntHistogram) _histograms[fieldIndex];
		    intHistogram.addValue(intValue);
		    break;
		case STRING_TYPE:
		    String strValue = ((StringField) field).getValue();
		    StringHistogram strHistogram = (StringHistogram) _histograms[fieldIndex];
		    strHistogram.addValue(strValue);
		    break;
		}
	    }
	    it.close();

	}catch(Exception e){
	    e.printStackTrace();
	    System.out.println("Error in populating Histogram for fieldIndex:" + fieldIndex);
	}
    }

}
