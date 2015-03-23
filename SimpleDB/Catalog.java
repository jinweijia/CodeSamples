package simpledb;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.*;

/**
 * The Catalog keeps track of all available tables in the database and their
 * associated schemas.
 * For now, this is a stub catalog that must be populated with tables by a
 * user program before it can be used -- eventually, this should be converted
 * to a catalog that reads a catalog table from disk.
 */

/*
 * The Table contains a DbFile object and its associated parameters. 
 */ 
class Table{

    DbFile _file;

    // Table ID of File object
    int _id;

    // Primary key field name
    String _pKey;

    // Name of the File
    String _name;

    /**
     * Construct a new Table.
     * 
     * @param file the contents of the table to add;  
     * @param id identifier of this file/tupledesc param for the calls getTupleDesc and getFile
     * @param name the name of the table -- may be an empty string.  May not be null.  
     * @param pkey the name of the primary key field
     * 
     */
    public Table(DbFile file, int id, String pKey, String name){
	_file = file;
	_id = id;
	_pKey = pKey;
	_name = name;
    }

    public int getID(){
	return _id;
    }

    public DbFile getDbFile(){
	return _file;
    }

    public String getPrimaryKey(){
	return _pKey;
    }

    public String getName(){
	return _name;
    }
}


public class Catalog {

    /**
     * Constructor.
     * Creates a new, empty catalog.
     */

    // Maps Table name to Table ID
    private HashMap<String, Integer> nameIdMap;

    // Maps Table ID to Table object
    private HashMap<Integer, Table> idCatalog;
    static int tableID;

    public Catalog() {
        nameIdMap = new HashMap<String, Integer>();
	idCatalog = new HashMap<Integer, Table>();
    }

    /**
     * Add a new table to the catalog.
     * This table's contents are stored in the specified DbFile.
     * @param file the contents of the table to add;  file.getId() is the identfier of
     *    this file/tupledesc param for the calls getTupleDesc and getFile
     * @param name the name of the table -- may be an empty string.  May not be null.  If a name
     * @param pkeyField the name of the primary key field
     * conflict exists, use the last table to be added as the table for a given name.
     */
    public void addTable(DbFile file, String name, String pkeyField) {
	   
	nameIdMap.put(name, file.getId());
	idCatalog.put(file.getId(), new Table(file, file.getId(), pkeyField, name));
	    
    }

    public void addTable(DbFile file, String name) {
        addTable(file, name, "");
    }

    /**
     * Add a new table to the catalog.
     * This table has tuples formatted using the specified TupleDesc and its
     * contents are stored in the specified DbFile.
     * @param file the contents of the table to add;  file.getId() is the identfier of
     *    this file/tupledesc param for the calls getTupleDesc and getFile
     */
    public void addTable(DbFile file) {
        addTable(file, (UUID.randomUUID()).toString());
    }

    /**
     * Return the id of the table with a specified name,
     * @throws NoSuchElementException if the table doesn't exist
     */
    public int getTableId(String name) throws NoSuchElementException {
        
	if(nameIdMap.get(name) == null){
	    throw new NoSuchElementException("No Table with name:" + name);
	}
        return nameIdMap.get(name);
    }

    /**
     * Returns the tuple descriptor (schema) of the specified table
     * @param tableid The id of the table, as specified by the DbFile.getId()
     *     function passed to addTable
     * @throws NoSuchElementException if the table doesn't exist
     */
    public TupleDesc getTupleDesc(int tableid) throws NoSuchElementException {
	if(idCatalog.get(tableid) == null){
	    throw new NoSuchElementException("No Table with ID:" + tableid);
	}
        Table tb = idCatalog.get(tableid);
	DbFile dbfile = tb.getDbFile();
        return dbfile.getTupleDesc();
    }

    /**
     * Returns the DbFile that can be used to read the contents of the
     * specified table.
     * @param tableid The id of the table, as specified by the DbFile.getId()
     *     function passed to addTable
     */
    public DbFile getDbFile(int tableid) throws NoSuchElementException {
	if(idCatalog.get(tableid) == null){
	    throw new NoSuchElementException("No Table with ID:" + tableid);
	}
        Table file = idCatalog.get(tableid);
        return file.getDbFile();
    }

    public String getPrimaryKey(int tableid) {
        Table tb = idCatalog.get(tableid);
        return tb.getPrimaryKey();
    }

    public Iterator<Integer> tableIdIterator() {
        Set<Integer> tableId = idCatalog.keySet();
        return tableId.iterator();
    }

    public String getTableName(int id) {
        Table tb = idCatalog.get(id);
        return tb.getName();
    }
    
    /** Delete all tables from the catalog */
    public void clear() {
	nameIdMap = new HashMap<String, Integer>();
	idCatalog = new HashMap<Integer, Table>();
    }
    
    /**
     * Reads the schema from a file and creates the appropriate tables in the database.
     * @param catalogFile
     */
    public void loadSchema(String catalogFile) {
        String line = "";
        File baseFolder=new File(catalogFile).getParentFile();
        try {
            BufferedReader br = new BufferedReader(new FileReader(new File(catalogFile)));
            
            while ((line = br.readLine()) != null) {
                //assume line is of the format name (field type, field type, ...)
                String name = line.substring(0, line.indexOf("(")).trim();
                //System.out.println("TABLE NAME: " + name);
                String fields = line.substring(line.indexOf("(") + 1, line.indexOf(")")).trim();
                String[] els = fields.split(",");
                ArrayList<String> names = new ArrayList<String>();
                ArrayList<Type> types = new ArrayList<Type>();
                String primaryKey = "";
                for (String e : els) {
                    String[] els2 = e.trim().split(" ");
                    names.add(els2[0].trim());
                    if (els2[1].trim().toLowerCase().equals("int"))
                        types.add(Type.INT_TYPE);
                    else if (els2[1].trim().toLowerCase().equals("string"))
                        types.add(Type.STRING_TYPE);
                    else {
                        System.out.println("Unknown type " + els2[1]);
                        System.exit(0);
                    }
                    if (els2.length == 3) {
                        if (els2[2].trim().equals("pk"))
                            primaryKey = els2[0].trim();
                        else {
                            System.out.println("Unknown annotation " + els2[2]);
                            System.exit(0);
                        }
                    }
                }
                Type[] typeAr = types.toArray(new Type[0]);
                String[] namesAr = names.toArray(new String[0]);
                TupleDesc t = new TupleDesc(typeAr, namesAr);
                HeapFile tabHf = new HeapFile(new File(baseFolder, name + ".dat"), t);
                addTable(tabHf,name,primaryKey);
                System.out.println("Added table : " + name + " with schema " + t);
            }
        } catch (IOException e) {
            e.printStackTrace();
            System.exit(0);
        } catch (IndexOutOfBoundsException e) {
            System.out.println ("Invalid catalog entry : " + line);
            System.exit(0);
        }
    }
}

