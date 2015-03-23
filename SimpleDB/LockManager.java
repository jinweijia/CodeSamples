package simpledb;
 
import java.io.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import java.lang.*;
 
/**
 * LockManager handles all lock and unlock requests
 * pageLocks manages a set of Metalocks, one lock for each Page
 * tranPages maps each transaction to the set of pages it has lock on
 */

public class LockManager {

    
    /** PageLocks maps PageId to pageId to PageId-unique MetaLocks*/
    ConcurrentHashMap<PageId, MetaLock> pageLocks;

    /** TranPages maps each transaction to the set of Pages which it has locks on*/
    ConcurrentHashMap<TransactionId, Set<PageId>> tranPages;   
    /**
     * Each Page has its own MetaLock.
     * Metalock manages all lock requests for that particular page.
     */
    class MetaLock {
     
	/** The set of transactionIds having read/shared lock on the Page*/
	Set<TransactionId> readtid;

	/** tid of transaction holding write/exclusive lock on Page */
	volatile TransactionId writetid;

	/** pid is unique for this MetaLock */
	PageId pid;

	/** Whether any transaction has writeLock on the Page */
	AtomicBoolean writes;
	
	/** The number of transactions holding readLock on the Page */
	AtomicInteger reads;	

	public MetaLock(PageId p) {

	    readtid = Collections.synchronizedSet(new HashSet<TransactionId>());
	    pid = p;
	    writes = new AtomicBoolean(false);
	    reads = new AtomicInteger(0);	    
	}

	/**
	 * Checks whether there are any lock, read or exclusive, held on this page. 
	 *
	 * @return boolean
	 *     Returns true if no lock held at all
	 */
	public boolean isVirgin() {

	    return (reads.get() == 0 && writes.get() == false);
	}
	 

	/**
	 * Checks whether there are any readLocks held. 
	 * 
	 * @return boolean
	 *     Whether any transaction hold read/shared lock on the page.Returns true if at least one transaction has read lock.
	 */
	public boolean isShared() {

	    return reads.get() > 0;
	}

	/**
	 * Checks if any transaction has exclusive writeLock on this page.
	 * 
	 * @return TransactionId
	 *      tid of transaction holding write/exclusive lock on Page
	 *      null if no such transaction
	 */
	public TransactionId isExclusive() {

	    if (writes.get() == false) {
		return null;
	    }
	    return writetid;
	}
	/**
	 * Function for transaction to request readLock on the Page's Metalock
	 * Multiple transaction can hold this lock.
	 *
	 * @param tid
	 *     Transaction requesting readlock
	 * @return boolean
	 *     Boolean to indicate whether the readlock has been granted
	 */

	public synchronized boolean readLock(TransactionId tid) {

	    if (isVirgin() || isShared()) {

		assert (writes.get() == false) : "Error in LockManager#readLock";
		if (!readtid.contains(tid)) {

		    readtid.add(tid);
		    reads.getAndIncrement();		    
		}
		return true;
	    } else if (isExclusive() != null && isExclusive().equals(tid)) {

		// Already have write lock for requesting tid requesting read lock
		return true;
	    }
	    // Lock has not been granted
	    return false;
	}

	/**
	 * Function for transaction to request writeLock on the Page's Metalock
	 * Only one transaction can hold this lock
	 *
	 * @param tid
	 *     Transaction requesting writelock
	 *
	 * @return boolean
	 *     Boolean to indicate whether the lock has been granted
	 */
	public synchronized boolean writeLock(TransactionId tid) {

	    if (isVirgin()) {

		writetid = tid;
		writes.set(true);
		return true;
	    } else if (!isShared() && isExclusive().equals(tid)) {

		// Already have write lock for requesting tid
		return true;
	    } else if (isShared()) {

		// Checks if lock upgrade conditions are met
		if (reads.get() == 1 && readtid.contains(tid)) {

		    // Upgrade read lock to write lock
		    readtid.remove(tid);
		    assert (readtid.isEmpty() == true) : "Error in LockManager#writeLock (lock upgrade)";
		    reads.getAndDecrement();
		    writetid = tid;
		    writes.set(true);

		    return true;
		}
	    }
	    return false;
	}

	/**
	 * Releases transaction's lock on the page 
	 *
	 * @param tid
	 *      Transaction releasing the locks
	 * @param readOnly
	 *      Whether we are only releasing readLocks, means writeLocks will not be released
	 */
	public synchronized void unlock(TransactionId tid, boolean readOnly) {

	    if (writes.get() == true) {

		if (readOnly == false) {
		    
		    writetid = null;
		    writes.set(false);		    
		}
	    } else if (isShared()) {

		readtid.remove(tid);
		reads.getAndDecrement();
	    }
	}


    }
 
 
    public LockManager() {
     
	pageLocks = new ConcurrentHashMap<PageId, MetaLock>();
	tranPages = new ConcurrentHashMap<TransactionId, Set<PageId>>();	
     
    }
    
    /**
     * Function for classes outside LockManager to call to acquire any lock 
     *
     * @param tid
     *      Transaction attempting to get lock
     * @param pid
     *      The page for which it is trying to get lock on
     * @param perm
     *      Readlock or Writelock
     * @return boolean
     *      Whether lock has been acquired
     */ 
    public boolean acquireLock(TransactionId tid, PageId pid, Permissions perm) {
	
	MetaLock l = pageLocks.get(pid);
	if (l == null) {
	    l = createLock(pid);
	}

	boolean result = false;
	if (perm == Permissions.READ_ONLY) {
	    
	    result = l.readLock(tid);
	    
	} else if (perm == Permissions.READ_WRITE) {	    
	    
	    result = l.writeLock(tid);    
	} else {
	    return false;
	}

	if (result == true) {

	    // If lock is acquired, add pageId to transaction pages
	    Set<PageId> s = tranPages.get(tid);
	    if (s == null) {
		s = createPidSet(tid);
	    }
	    s.add(pid);
	}
	return result;
    }
       

    /**
     * Gets the MetaLock for the page or creates a new one for a page given PageId
     * 
     * @param p
     *       The page for which the Metalock is attached to
     * @return MetaLock
     *       Metalock for the page
     */
    private synchronized MetaLock createLock(PageId p) {

	// Check if Metalock exists for PageId p
	if (pageLocks.get(p) == null) {

	    //Creates a new Metalock for PageId p
	    MetaLock l = new MetaLock(p);
	    pageLocks.put(p, l);

	}
	return pageLocks.get(p);
    }
 
    /**
     * Get or create the set of PageIds which the Transaction has hold locks on.
     *
     * @param tid
     *       Transaction for which this set of pageId belong
     * @return Set<PageId>
     *       Set of PageIds which the Transaction has hold locks on for tranPages.
     */
    private synchronized Set<PageId> createPidSet(TransactionId tid) {

	// Check if pid set exists for TransactionId tid
	if (tranPages.get(tid) == null) {
	    
	    Set<PageId> sp = Collections.synchronizedSet(new HashSet<PageId>());
	    tranPages.put(tid, sp);
	}
	return tranPages.get(tid);
    }
    
    /**
     * Releases all the locks for one particular transaction
     *
     * @param tid
     *       The transaction to release all the locks
     */
    public void releaseTranLocks(TransactionId tid){

	Set<PageId> pidSet = tranPages.get(tid);
	if(pidSet != null){

	    PageId[] pa = pidSet.toArray(new PageId[0]);
	    // Loops through the set of Pages which the transaction has lock on
	    for(PageId pid: pa){
		// Releases the lock on each page
		releasePageLock(tid, pid, false);
	    }
	    tranPages.remove(tid);
	}
    }
 
    /**
     * Releases lock for a transactin on a particular page
     * @param tid 
     *      Transaction holding the lock to be released
     * @param pid
     *      The pageId of the metalock
     * @param readOnly
     *      Whether we are only releasing readLocks, means we don't release writeLock if there is
     */
 
    public void releasePageLock(TransactionId tid, PageId pid, boolean readOnly){	

	MetaLock l = pageLocks.get(pid);
	if(l != null){

	    l.unlock(tid, readOnly);
	    Set<PageId> s = tranPages.get(tid);
	    s.remove(pid);
	}
    }

    
    public boolean isLocked(TransactionId tid, PageId pid){
	
	MetaLock lock = pageLocks.get(pid);
	if (lock == null) {
	    return false;
	}
	return !lock.isVirgin();
 
    }
    /**
     * Gets the set of PageIds for which the transaction has locks on
     *
     * @param tid
     *      TransactionId tid
     */

    public Set<PageId> getTranPages(TransactionId tid){

	return tranPages.get(tid);
    }


}
 
