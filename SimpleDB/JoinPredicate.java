package simpledb;

import java.io.Serializable;

/**
 * JoinPredicate compares fields of two tuples using a predicate. JoinPredicate
 * is most likely used by the Join operator.
 */
public class JoinPredicate implements Serializable {

    private static final long serialVersionUID = 1L;

    /**
     * Constructor -- create a new predicate over two fields of two tuples.
     * 
     * @param field1
     *            The field index into the first tuple in the predicate
     * @param field2
     *            The field index into the second tuple in the predicate
     * @param op
     *            The operation to apply (as defined in Predicate.Op); either
     *            Predicate.Op.GREATER_THAN, Predicate.Op.LESS_THAN,
     *            Predicate.Op.EQUAL, Predicate.Op.GREATER_THAN_OR_EQ, or
     *            Predicate.Op.LESS_THAN_OR_EQ
     * @see Predicate
     */

    int _field1;
    Predicate.Op _op;
    int _field2;

    public JoinPredicate(int field1, Predicate.Op op, int field2) {
	_field1 = field1;
	_op = op;
	_field2 = field2;
    }

    /**
     * Apply the predicate to the two specified tuples. The comparison can be
     * made through Field's compare method.
     * 
     * @return true if the tuples satisfy the predicate.
     */
    public boolean filter(Tuple t1, Tuple t2){
	Field f1 = t1.getField(_field1);
	Field f2 = t2.getField(_field2);
        return f1.compare(_op, f2);
    }
    
    public int getField1()
    {
        return _field1;
    }
    
    public int getField2()
    {
        return _field2;
    }
    
    public Predicate.Op getOperator()
    {
        // some code goes here
        return _op;
    }
}
