package simpledb;

import java.io.*;
import java.util.*;
import java.lang.*;
import java.util.concurrent.*;

/**
 * BufferPool manages the reading and writing of pages into memory from
 * disk. Access methods call into it to retrieve pages, and it fetches
 * pages from the appropriate location.
 * <p>
 * The BufferPool is also responsible for locking;  when a transaction fetches
 * a page, BufferPool checks that the transaction has the appropriate
 * locks to read/write the page.
 */
public class BufferPool {
    /** Bytes per page, including header. */
    public static final int PAGE_SIZE = 4096;

    /** Default number of pages passed to the constructor. This is used by
    other classes. BufferPool should use the numPages argument to the
    constructor instead. */
    public static final int DEFAULT_PAGES = 50;   

    /** Maximum number of Pages that the buffer can hold*/
    public static int MAX_CAPACITY;

    // Contains the lock for each page and dispense locks
    private LockManager lockManager;

    // Cache of pages in the buffer, mapped by their page id
    private Map<PageId, Page> bufferPool;

 
    /**
     * Creates a BufferPool that caches up to numPages pages.
     *
     * @param numPages maximum number of pages in this buffer pool.
     */  
    public BufferPool(int numPages) {
        
	MAX_CAPACITY = numPages;
	bufferPool = Collections.synchronizedMap(new LinkedHashMap<PageId, Page>(numPages+1, 1.0f, true));
	lockManager = new LockManager();
    }

    /**
     * Retrieve the specified page with the associated permissions.
     * Will acquire a lock and may block if that lock is held by another
     * transaction.
     * <p>
     * The retrieved page should be looked up in the buffer pool.  If it
     * is present, it should be returned.  If it is not present, it should
     * be added to the buffer pool and returned.  If there is insufficient
     * space in the buffer pool, an page should be evicted and the new page
     * should be added in its place.
     *
     * @param tid the ID of the transaction requesting the page
     * @param pid the ID of the requested page
     * @param perm the requested permissions on the page
     */
    public Page getPage(TransactionId tid, PageId pid, Permissions perm)
        throws TransactionAbortedException, DbException {	
	
	boolean cached = bufferPool.containsKey(pid);
	
	if(!cached){
	    
	    // If it is not in bufferpool, read from disk and add to buffer
	    synchronized(this) {

		// Check again to see if the requested page is cached
		if (!bufferPool.containsKey(pid)) {
		    int tableId = pid.getTableId();
		    DbFile dbfile = Database.getCatalog().getDbFile(tableId);
		    Page newPage = dbfile.readPage(pid);
	    
		    while (bufferPool.size() >= MAX_CAPACITY){
		    	evictPage();
		    }
		    bufferPool.put(pid, newPage);
		}	    
	    }
	}

	int count = 0;
	Random rng = new Random();
	while (count < 30) {

	    // Attempt to acquire lock at most 30 times, with a short sleep interval (0-33 ms) in between
	    if (lockManager.acquireLock(tid, pid, perm) == false) {
		try {
		    Thread.sleep(rng.nextInt(33));
		} catch (InterruptedException e) {
		    e.printStackTrace();
		}
		count++;
	    } else {
		return bufferPool.get(pid);
	    }
	}
	// Fails to acquire lock, abort transaction
	throw new TransactionAbortedException();
    }

    /**
     * Releases the lock on a page.
     * Calling this is very risky, and may result in wrong behavior. Think hard
     * about who needs to call this and why, and why they can run the risk of
     * calling it.
     *
     * @param tid the ID of the transaction requesting the unlock
     * @param pid the ID of the page to unlock
     */
    public void releasePage(TransactionId tid, PageId pid) {
        
        lockManager.releasePageLock(tid,pid,false);
    }

    /**
     * Returns the lock manager for this bufferpool instance
     */ 
    public LockManager getLockManager() {

	return lockManager;
    }

    /**
     * Release all locks associated with a given transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     */
    public void transactionComplete(TransactionId tid) throws IOException {
        
	transactionComplete(tid,true);
    }

    /** Return true if the specified transaction has a lock on the specified page */
    public boolean holdsLock(TransactionId tid, PageId p) {
        // some code goes here
        
        return lockManager.isLocked(tid, p);
    }

    /**
     * Commit or abort a given transaction; release all locks associated to
     * the transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     * @param commit a flag indicating whether we should commit or abort
     */
    public void transactionComplete(TransactionId tid, boolean commit)
        throws IOException {        
	
	if (lockManager.getTranPages(tid) != null) {
	    // For each page used by the transaction, flush them to disk if commit. Otherwise, replace with unmodified version 
	    PageId[] pit = lockManager.getTranPages(tid).toArray(new PageId[0]);
	    synchronized(bufferPool) {
	    
		for (PageId p: pit) {
	   
		    if (bufferPool.containsKey(p)) {

			Page pg = bufferPool.get(p);
			if (pg.isDirty() != null) {

			    if (commit) {

				flushPage(p);			    
			    } else {			    

				// Replace cached page with on-disk version			    
				Page dirty = bufferPool.put(p, pg.getBeforeImage());
				pg.markDirty(false, null);
			    }
			}
		    }
		}
	    }
	}

	lockManager.releaseTranLocks(tid);
    }

    /**
     * Add a tuple to the specified table behalf of transaction tid.  Will
     * acquire a write lock on the page the tuple is added to(Lock 
     * acquisition is not needed for lab2). May block if the lock cannot 
     * be acquired.
     * 
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit, and updates cached versions of any pages that have 
     * been dirtied so that future requests see up-to-date pages. 
     *
     * @param tid the transaction adding the tuple
     * @param tableId the table to add the tuple to
     * @param t the tuple to add
     */
    public void insertTuple(TransactionId tid, int tableId, Tuple t)
        throws DbException, IOException, TransactionAbortedException {
        // some code goes here

	DbFile dbfile = Database.getCatalog().getDbFile(tableId);

	
	ArrayList<Page> modified = dbfile.insertTuple(tid, t);

	for (int i=0; i<modified.size(); i++) {

	    Page p = modified.get(i);
	    
	    // Marks the page dirty because we have already edited it
	    p.markDirty(true, tid);

	    PageId pid = p.getId(); 
	    boolean cached = bufferPool.containsKey(pid);

	    if(cached){
		
		bufferPool.put(pid, p);
	    }
	}
	    
    }

    /**
     * Remove the specified tuple from the buffer pool.
     * Will acquire a write lock on the page the tuple is removed from. May block if
     * the lock cannot be acquired.
     *
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit.  Does not need to update cached versions of any pages that have 
     * been dirtied, as it is not possible that a new page was created during the deletion
     * (note difference from addTuple).
     *
     * @param tid the transaction adding the tuple.
     * @param t the tuple to add
     */
    public void deleteTuple(TransactionId tid, Tuple t)
        throws DbException, TransactionAbortedException {
        // some code goes here

	DbFile dbfile = Database.getCatalog().getDbFile(t.getRecordId().getPageId().getTableId());

	
	Page p = dbfile.deleteTuple(tid, t);

	p.markDirty(true, tid);
	    
	
    }

    /**
     * Flush all dirty pages to disk.
     * NB: Be careful using this routine -- it writes dirty data to disk so will
     *     break simpledb if running in NO STEAL mode.
     */
    public synchronized void flushAllPages() throws IOException {
        // some code goes here
      
	Collection<PageId> k = bufferPool.keySet();
	PageId[] it = k.toArray(new PageId[0]);
        for (PageId pid : it) {
	    
	    flushPage(pid);
	}

    }

    /** Remove the specific page id from the buffer pool.
        Needed by the recovery manager to ensure that the
        buffer pool doesn't keep a rolled back page in its
        cache.
    */
    public synchronized void discardPage(PageId pid) {
        // some code goes here
	// not necessary for proj1

	bufferPool.remove(pid);
    }

    /**
     * Flushes a certain page to disk
     * @param pid an ID indicating the page to flush
     */
    private synchronized  void flushPage(PageId pid) throws IOException {
        // some code goes here

	Page p = bufferPool.get(pid);
	TransactionId tid = p.isDirty();

	if (tid != null) {

	    DbFile file = Database.getCatalog().getDbFile(pid.getTableId());
	    file.writePage(p);
	    p.markDirty(false, null);

	}

    }

    /** Write all pages of the specified transaction to disk.
     */
    public synchronized  void flushPages(TransactionId tid) throws IOException {
        // some code goes here
        // not necessary for proj1
    }

    /**
     * Discards a page from the buffer pool.
     * Flushes the page to disk to ensure dirty pages are updated on disk.
     */
    private synchronized  void evictPage() throws DbException {
        // some code goes here

	PageId[] it = bufferPool.keySet().toArray(new PageId[0]);	

	// We always remove the last one on the list because the latest accessed page is always at the end of the list.

	for (int i = it.length-1; i >= 0; i--) {

	    PageId pid = it[i];
	    if (bufferPool.get(pid).isDirty() == null) {

		try {
	    
		    flushPage(pid);	    
		} catch (IOException e) {

		    e.printStackTrace();	    	
		    System.out.println("Exception in evictPage#flushPage");
		}
		bufferPool.remove(pid);
		return;
	    }
	}
	throw new DbException("Error in BufferPool#evictPage: all pages are dirty");
	

    }

}
