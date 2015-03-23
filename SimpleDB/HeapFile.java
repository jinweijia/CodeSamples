package simpledb;

import java.io.*;
import java.util.*;
import java.util.concurrent.atomic.*;
import java.lang.*;

/**
 * HeapFile is an implementation of a DbFile that stores a collection of tuples
 * in no particular order. Tuples are stored on pages, each of which is a fixed
 * size, and the file is simply a collection of those pages. HeapFile works
 * closely with HeapPage. The format of HeapPages is described in the HeapPage
 * constructor.
 * 
 * @see simpledb.HeapPage#HeapPage
 * @author Sam Madden
 */
public class HeapFile implements DbFile {

    private File _file;
    private TupleDesc _tupleDesc;
    private int fileId;
    private AtomicInteger pageNum;
    private AtomicLong _pgLen;

    /** Provides random access to the File _file with specific offset*/
    private RandomAccessFile _raf; 
   

    /**
     * Constructs a heap file backed by the specified file.
     * 
     * @param f
     *            the file that stores the on-disk backing store for this heap
     *            file.
     */
    public HeapFile(File f, TupleDesc td) {
        _file = f;
	_tupleDesc = td;
	fileId = _file.getAbsoluteFile().hashCode();	
	try {
	    _raf = new RandomAccessFile(_file, "r");
	    _pgLen = new AtomicLong(_raf.length());
	    _raf.close();
	} catch (Exception e) {
	    e.printStackTrace();
	    System.out.println("Exception in HeapFile constructor");
	}
	pageNum = new AtomicInteger(this.numPages());
	
	
    }

    /**
     * Returns the File backing this HeapFile on disk.
     * 
     * @return the File backing this HeapFile on disk.
     */
    public File getFile() {
        
        return _file;
    }

    /**
     * Returns an ID uniquely identifying this HeapFile. Implementation note:
     * you will need to generate this tableid somewhere ensure that each
     * HeapFile has a "unique id," and that you always return the same value for
     * a particular HeapFile. We suggest hashing the absolute file name of the
     * file underlying the heapfile, i.e. f.getAbsoluteFile().hashCode().
     * 
     * @return an ID uniquely identifying this HeapFile.
     */
    public int getId() {
    
	return _file.getAbsoluteFile().hashCode();
    }

    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     * 
     * @return TupleDesc of this DbFile.
     */
    public TupleDesc getTupleDesc() {
        return _tupleDesc;
    }

    
    // see DbFile.java for javadocs
    public Page readPage(PageId pid) { 

	/** Array of bytes to where page data will be written to in readPage()*/
	byte[] _pgData = new byte[BufferPool.PAGE_SIZE];

	/** Number of bytes written to _pgData in readPage()*/
	int _pgBytes;

	Page newPage = null;

	if (pid.getTableId() == this.getId()) {
	    
	    int offset = BufferPool.PAGE_SIZE * pid.pageNumber();
	    try {
	    	_raf = new RandomAccessFile(_file, "r");
		
		// Accessing file at offset
	    	_raf.seek(offset);

		// Read page data from randomAccessFile
	    	_pgBytes = _raf.read(_pgData);

		// Close the randomAccessFile
	    	_raf.close();	    
	    	newPage = new HeapPage((HeapPageId) pid, _pgData);

	    }catch (Exception e) {	    
	    	e.printStackTrace();	    	
	    	System.out.println("Exception in readPage() in HeapFile");
	    }
	    return newPage;

	}else{
	    throw new IllegalArgumentException();
	}
    
    }

    // see DbFile.java for javadocs
    public void writePage(Page page) throws IOException {
        // some code goes here
       
	int offset = BufferPool.PAGE_SIZE * page.getId().pageNumber();

	try {
	    _raf = new RandomAccessFile(_file, "rw");
		
	    // Accessing file at offset	        
	    _raf.seek(offset);

	    // Write page to file starting at offset
	    _raf.write(page.getPageData());
	    _pgLen.set(_raf.length());

	    // Close the randomAccessFile
	    _raf.close();	    
	    
	}catch (Exception e) {	    

	    e.printStackTrace();	    	
	    System.out.println("Exception in writePage() in HeapFile");

	}
	
    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
        // some code goes here

	return (int) Math.ceil( _pgLen.get() /  BufferPool.PAGE_SIZE);
    }
    
    // see DbFile.java
    public ArrayList<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {

	ArrayList<Page> changed = new ArrayList<Page>();	
	
	//Loops through all Pages to find empty slots for insertion, and insert at the earliest found empty slot
	synchronized(this) {
	    for (int i=0; i<numPages(); i++) {

		PageId pid = new HeapPageId(fileId, i);
		HeapPage next = (HeapPage) Database.getBufferPool().getPage(tid, pid, Permissions.READ_ONLY);
		int slots = next.getNumEmptySlots();
		if (slots <= 0) {

		    // If this page is full, release read lock immediately
		    Database.getBufferPool().getLockManager().releasePageLock(tid, pid, true);
		} else {

		    // Otherwise, obtain write lock on the page and insert tuple
		    next = (HeapPage) Database.getBufferPool().getPage(tid, pid, Permissions.READ_WRITE);
		    next.insertTuple(t);		
		    changed.add((Page) next);
		    return changed;
		}
	    }
	    // If no empty page is found, then create a new HeapPage and write it to disk
	    HeapPage newpg = new HeapPage(new HeapPageId(fileId, numPages()), HeapPage.createEmptyPageData());	
	    writePage(newpg);
	    pageNum.getAndIncrement();
	    // Call insertTuple helper method to insert tuple into the newly created page
	    return insertTupletoPage(tid, t, newpg.getId());
	}
        
    }


    private ArrayList<Page> insertTupletoPage(TransactionId tid, Tuple t, PageId pid)
            throws DbException, IOException, TransactionAbortedException {

	ArrayList<Page> changed = new ArrayList<Page>();
	HeapPage next = (HeapPage) Database.getBufferPool().getPage(tid, pid, Permissions.READ_WRITE);
	next.insertTuple(t);		
	changed.add((Page) next);
	return changed;
    }


    // see DbFile.java for javadocs
    public Page deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {
        // some code goes here

	//Gets the relevant pageId to find the heapPage which contains the tuple, and delete it from that page

	PageId pid = t.getRecordId().getPageId();
	HeapPage p = (HeapPage) Database.getBufferPool().getPage(tid, pid, Permissions.READ_WRITE);
	p.deleteTuple(t);
	
        return ((Page) p);
        
    }

    // see DbFile.java for javadocs
    public DbFileIterator iterator(TransactionId tid) {
	
        return new PageIterator(this, tid);
    }

    /**
     * Iterator over all tuples in all pages in the HeapFile
     * @see simpledb.DbFileIterator
     */
    class PageIterator implements DbFileIterator {


	int maxPages;
	TransactionId tid;
	HeapFile hf;

	/** Current page that we are iterating over */
	Page current;

	/** Current page number of page we are iterating on */
	int progress = 0;

	/** Tuple iterator over tuples in the current page */
	Iterator<Tuple> tupleIter;

	/**
	 * Constructs the iterator
	 * @param h HeapFile we are constructing iterator on
	 * @param tid TransactionId of transaction requesting current iterator
	 */
	public PageIterator(HeapFile h, TransactionId tid) {
	    this.maxPages = h.numPages();
	    this.tid = tid;
	    this.hf = h;
	}

	/**
	 * Load new page from buffer
	 * @return A page to be iterated over next
	 */
	private Page loadPage() throws DbException, TransactionAbortedException {
	    	     		
	    PageId pid = new HeapPageId(hf.getId(), progress);	   
	    Page newPage = Database.getBufferPool().getPage(tid, pid, Permissions.READ_ONLY);
	    progress++;
	    return newPage;
	} 

	public void open() throws DbException, TransactionAbortedException {
	    progress=0;
	    current = loadPage();	    
	    tupleIter = ((HeapPage) current).iterator();	   
	}

	public boolean hasNext() throws DbException, TransactionAbortedException {
	    if (tupleIter == null || current == null) {
		return false;
	    }

	    if (tupleIter.hasNext()) {
		return true;
	    } else if (!tupleIter.hasNext() && progress < maxPages) {
		int tempNextPid = progress;
		PageId pid = new HeapPageId(hf.getId(), tempNextPid);	   
		Page newPage = Database.getBufferPool().getPage(tid, pid, Permissions.READ_ONLY);
		if (((HeapPage) newPage).iterator().hasNext()) { 
		    return true;
		}
	    }
	   
	    return false;	    
	    
	}
	
	    
	public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
	    if (this.hasNext()) {
		
		if (tupleIter.hasNext()) {
		    // If there are still tuples left in tupleIter, return the next tuple in tupleIter
		    return tupleIter.next();
		    
		} else {
		    // Otherwise, load the next page in the HeapFile
		    current = loadPage();
		    tupleIter = ((HeapPage) current).iterator();
		    if (tupleIter.hasNext()) {
			return tupleIter.next();
		    } else {
			throw new NoSuchElementException("New page loaded is empty");
		    }
		}
	    } else {
		throw new NoSuchElementException();
	    }
	}

	public void rewind() throws DbException, TransactionAbortedException {
	    close();
	    open();
	}

	public void close() {
	    tupleIter = null;	    
	}

    }

}

