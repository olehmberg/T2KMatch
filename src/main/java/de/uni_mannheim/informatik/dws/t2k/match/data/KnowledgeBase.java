package de.uni_mannheim.informatik.dws.t2k.match.data;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Serializable;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.joda.time.DateTime;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

import de.uni_mannheim.informatik.dws.t2k.index.dbpedia.DBpediaIndexer;
import de.uni_mannheim.informatik.dws.winter.index.IIndex;
import de.uni_mannheim.informatik.dws.winter.model.DataSet;
import de.uni_mannheim.informatik.dws.winter.model.ParallelHashedDataSet;
import de.uni_mannheim.informatik.dws.winter.preprocessing.datatypes.DataType;
import de.uni_mannheim.informatik.dws.winter.utils.MapUtils;
import de.uni_mannheim.informatik.dws.winter.webtables.Table;
import de.uni_mannheim.informatik.dws.winter.webtables.TableColumn;
import de.uni_mannheim.informatik.dws.winter.webtables.TableRow;
import de.uni_mannheim.informatik.dws.winter.webtables.lod.LodTableColumn;
import de.uni_mannheim.informatik.dws.winter.webtables.parsers.LodCsvTableParser;


/**
 * 
 * Model of a knowledge base.
 * 
 * @author Oliver Lehmberg (oli@dwslab.de)
 *
 */
public class KnowledgeBase implements Serializable {

	private static final long serialVersionUID = 1L;
	
	// data that will be matched: records and schema
	private DataSet<MatchableTableRow, MatchableTableColumn> records = new ParallelHashedDataSet<>();
	private DataSet<MatchableTableColumn, MatchableTableColumn> schema = new ParallelHashedDataSet<>();
	private DataSet<MatchableTable, MatchableTableColumn> tables = new ParallelHashedDataSet<>();
	
	// translation for DBpedia property URIs: URI string to integer
	private LinkedList<String> properties = new LinkedList<>();
	private HashMap<String, Integer> propertyIds = new HashMap<>();
    
	// translation from property id to column index per DBpedia class
	private Map<Integer, Map<Integer, Integer>> propertyIndices = new HashMap<>();
	// translation from column index to global property id per DBpedia class
	private Map<Integer, Map<Integer, Integer>> propertyIndicesInverse = new HashMap<>();
	
	private Map<Integer, DateTime[]> dateRanges = new HashMap<>();
	
	// translation from table file name to table id
	private Map<String, Integer> tableIds = new HashMap<>();
	
	//translation from table id to DBpedia class
	private Map<Integer, String> classIndices = new HashMap<>();
	
	// translation from class name to table id
	private Map<String, Integer> classToId = new HashMap<>();
	
	// rdfs:label
	private MatchableLodColumn rdfsLabel;
	
	// lookup for tables by id
	private HashMap<Integer, Integer> sizePerTable = new HashMap<Integer, Integer>();
	
	// class weights
	private HashMap<Integer, Double> classWeight = new HashMap<Integer, Double>();
	
	//class hierarchy
	private static HashMap<String, String> classHierarchy = new HashMap<String, String>();
	/**
	 * @return the classHierarchy mapping class -> super class
	 */
	public static HashMap<String, String> getClassHierarchy() {
		return classHierarchy;
	}
	
	private static boolean doSerialise = true;
	public static void setDoSerialise(boolean serialise) {
		doSerialise = serialise;
	}
	
	public static KnowledgeBase loadKnowledgeBase(File location, IIndex index, SurfaceForms sf) throws FileNotFoundException {
		// look for serialised version
		File ser = new File(location.getParentFile(), location.getName() + ".bin");
		
		// index from serialised version is not implemented, so only load serialised if we did not get an index to fill
    	if(index==null) {
    		if(ser.exists()) {
				return KnowledgeBase.deserialise(ser);
    		} else if(location.getName().endsWith(".bin")) {
    			return KnowledgeBase.deserialise(location); 
    		}
    	}
    	
    	// load KB from location
    	KnowledgeBase kb = new KnowledgeBase();		
		kb.load(location, index, sf);

		// serialise
		if(doSerialise) {
			kb.serialise(ser);
		}
		
		return kb;
	}
	
	public void load(File location, IIndex index, SurfaceForms sForms) {
    	/***********************************************
    	 * Load DBpedia
    	 ***********************************************/

    	LodCsvTableParser lodParser = new LodCsvTableParser();
    	DBpediaIndexer indexer = new DBpediaIndexer();
    	
    	List<File> dbpFiles = null;
    	
    	if(location.isDirectory()) {
			dbpFiles = Arrays.asList(location.listFiles());
    	} else {
    		dbpFiles = Arrays.asList(new File[] { location});
    	}
    	
    	int tblIdx = 0;
    	for(File f : dbpFiles) {
			System.out.println("Loading Knowledge Base Table " + f.getName());
			Table tDBp = lodParser.parseTable(f);
			tDBp.setTableId(tblIdx);
			String className = tDBp.getPath().replace(".csv", "").replace(".gz", "");
			
			MatchableTable mt = new MatchableTable(tblIdx, className);
			tables.add(mt);
			tableIds.put(className, tblIdx);			
			
			if(tDBp.getSchema().getSize()>1 && "rdf-schema#label".equals(tDBp.getSchema().get(1).getHeader())) {
				
				tDBp.setSubjectColumnIndex(1);
	    		
				if(dbpFiles.size()==1) {
					for(TableColumn tc : tDBp.getSchema().getRecords()) {
						System.out.println(String.format("{%s} [%d] %s (%s): %s", tDBp.getPath(), tc.getColumnIndex(), tc.getHeader(), tc.getDataType(), tc.getUri()));
					}
				}
	    		
	    		// remove object properties and keep only "_label" columns (otherwise we will have duplicate property URLs)
	    		LodTableColumn[] cols = tDBp.getColumns().toArray(new LodTableColumn[tDBp.getSchema().getSize()]);
	    		List<Integer> removedColumns = new LinkedList<>();
	    		for(LodTableColumn tc : cols) {
	    			if(tc.isReferenceLabel()) {
	    				Iterator<TableColumn> it = tDBp.getSchema().getRecords().iterator();
	    				
	    				while(it.hasNext()) {
	    					LodTableColumn ltc = (LodTableColumn)it.next();
	    					
	    					if(!ltc.isReferenceLabel() && ltc.getUri().equals(tc.getUri())) {
	    						it.remove();
	    						removedColumns.add(ltc.getColumnIndex());
	    					}
	    				}
	    			}
	    		}
	    		
	    		// re-create value arrays
	    		for(TableRow r : tDBp.getRows()) {
	    			Object[] values = new Object[tDBp.getSchema().getSize()];
	    			
	    			int newIndex = 0;
	    			for(int i=0; i < r.getValueArray().length; i++) {
	    				if(!removedColumns.contains(i)) {
	    					values[newIndex++] = r.getValueArray()[i];
	    				}
	    			}
	    			
	    			r.set(values);
	    		}
	    		
	    		// create (unified) dbp schema and assign a number to each URI
	    		// create translation table from global property id to column index
	    		HashMap<Integer, Integer> indexTranslation = new HashMap<>();
	    		HashMap<Integer, Integer> indexTranslationInverse = new HashMap<>();
	    		int colIdx=0;
	    		for(TableColumn tc : tDBp.getSchema().getRecords()) {
	    			if(!propertyIds.containsKey(tc.getUri())) {
		    			int globalId = properties.size();
		    			propertyIds.put(tc.getUri(), globalId);
		    			properties.add(tc.getUri());
		    			
		    			indexTranslation.put(globalId, colIdx);
		    			indexTranslationInverse.put(colIdx, globalId);
		    			//here for each column (property) table Id will be '0' because there is no particular column (property) that belongs to one particular Dbpedia class.  
		    			MatchableLodColumn mc = new MatchableLodColumn(0, tc, globalId);
		    			schema.add(mc);
		    			
		    			if(tc.getUri().equals("http://www.w3.org/2000/01/rdf-schema#label")) {
		    				rdfsLabel = mc;
		    			}
	    			} else {
		    			Integer globalPropertyId = propertyIds.get(tc.getUri());
		    			// translate DBpedia-wide id (globalPropertyId) to index in type and value array (colIdx)
		    			// (indexTranslation is per table)
		    			indexTranslation.put(globalPropertyId, colIdx);
		    			indexTranslationInverse.put(colIdx, globalPropertyId);
	    			}
	    			
	    			colIdx++;
	    		}
	    		
	    		propertyIndices.put(tblIdx, indexTranslation);
	    		propertyIndicesInverse.put(tblIdx, indexTranslationInverse);
	    		
		    	for(TableRow r : tDBp.getRows()) {
		    		// make sure only the instance with the most specific class (=largest number of columns) remains in the final dataset for each URI
		    		MatchableTableRow mr = records.getRecord(r.getIdentifier());
		    		
		    		if(mr==null) {
		    			mr = new MatchableTableRow(r, tblIdx);
		    		} else {
		    			String clsOfPrevoisRecord = classIndices.get(mr.getTableId());
		    			String clsOfCurrentRecord = tDBp.getPath().replace(".csv", "").replace(".gz", "");
		    			
		    			if(classHierarchy.get(clsOfPrevoisRecord)==null){
		    				continue;
		    			}else {
		    				String cls;
		    				boolean flag = false;
		    				while((cls = classHierarchy.get(clsOfPrevoisRecord)) != null){
		    					if(cls.equals(clsOfCurrentRecord)){
		    						flag = true;
		    						break;
		    					}else{
		    						clsOfPrevoisRecord = cls;
		    					}
		    				}
		    				if(flag == false){
		    					mr = new MatchableTableRow(r, tblIdx);
		    				}
		    			}
		    				
		    		}
		    		
		    		records.add(mr);
		    	}
		    	sizePerTable.put(tblIdx, tDBp.getSize());
		    	
		    	String clsName = tDBp.getPath().replace(".csv", "").replace(".gz", "");
		    	classIndices.put(tblIdx, clsName);
		    	classToId.put(clsName, tblIdx);
		    	
		    	// we don't need the table rows anymore (MatchableTableRow is used from now on)
		    	tDBp.clear();
		    	tblIdx++;
			} else {
				System.out.println(" -> no key!");
			}
    	}
    	
    	// add classes from the class hierarchy which have not been loaded (but can be mapped via the hierarchy)
    	for(String cls : new HashSet<>(classToId.keySet())) {
    		
    		String superClass = classHierarchy.get(cls);
    		
    		while(superClass!=null) {
    			
    			if(!classToId.containsKey(superClass)) {
    				MatchableTable mt = new MatchableTable(tblIdx, superClass);
    				tables.add(mt);
    				classToId.put(superClass, tblIdx);
    				classIndices.put(tblIdx, superClass);
    				tblIdx++;
    			}
    			
    			superClass = classHierarchy.get(superClass);
    		}
    		
    	}
    	
    	LodCsvTableParser.endLoadData();
    	
//    	calculate class weights
    	calculateClassWeight();
    	
    	determineDateRanges();
    	
		if(index!=null) {
			System.out.println("Indexing ...");
			indexer.indexInstances(index, records.get(), classIndices, sForms);
		}
    	
    	System.out.println(String.format("%,d DBpedia Instances loaded from CSV", records.size()));
    	System.out.println(String.format("%,d DBpedia Properties / %,d Property IDs", schema.size(), propertyIds.size()));
    }
	
	public static void loadClassHierarchy(String location) throws IOException{
		System.out.println("Loading Class Hierarchy...");
		BufferedReader tsvReader = new BufferedReader(new InputStreamReader(new FileInputStream(new File(location))));
		String values;
		while((values = tsvReader.readLine()) != null){
			String[] cls = values.split("\t")[0].split("/");
			String[] superCls = values.split("\t")[1].split("/");
			classHierarchy.put(cls[cls.length-1].replaceAll("\"", ""), superCls[superCls.length-1].replaceAll("\"", ""));
		}
		tsvReader.close();
		System.out.println("Loaded Class Hierarchy for " + classHierarchy.size() + " Resources.");
	}
	
	public static KnowledgeBase deserialise(File location) throws FileNotFoundException {
		System.out.println("Deserialising Knowledge Base");
		
        Kryo kryo = KryoFactory.createKryoInstance();
        
        Input input = new Input(new FileInputStream(location));
        KnowledgeBase kb = kryo.readObject(input, KnowledgeBase.class);
        input.close();
        
        return kb;
	}
	
	public void serialise(File location) throws FileNotFoundException {
		System.out.println("Serialising Knowledge Base");
		
        Kryo kryo = KryoFactory.createKryoInstance();
        Output output = new Output(new FileOutputStream(location));
        kryo.writeObject(output, this);
        output.close();
	}
    
    public void calculateClassWeight(){
		double max = -1;
        
	      for (Entry<Integer, Integer> tableSize : getTablesSize().entrySet()) {
	            if (tableSize.getValue() < 1) {
	                continue;
	            }
	            if (tableSize.getValue() > max) {
	                max = tableSize.getValue();
	            }
	        }      
	        
	        for(Entry<Integer, Integer> tableSize : getTablesSize().entrySet()){
	        	double value = 0;
	        	if (tableSize.getValue() < 1) {
	                value = 1;
	            }
	            value =tableSize.getValue()/max;
	            value = 1-value;
	            classWeight.put(tableSize.getKey(), value);
	            }
	        
		
	}
    
    public void determineDateRanges() {
    	for(MatchableTableRow row : records.get()) {
    		
    		for(MatchableTableColumn col : schema.get()) {
    			
    			if(col.getType()==DataType.date) {
    				
    				DateTime[] range = MapUtils.get(dateRanges, col.getColumnIndex(), new DateTime[2]);
    				
    				Map<Integer, Integer> indexTranslation = getPropertyIndices().get(row.getTableId());
    				if(indexTranslation==null) {
    					System.err.println("Missing property index translation for table " + row.getTableId());
    				}
    				
//    				'secondColumnIndex' ('globalId' of dbpedia property) is used to get 'columnIndex' of dbpedia property in a respective table
    				Integer translatedIndex = indexTranslation.get(col.getColumnIndex());
    				if(translatedIndex!=null) {

	    				Object obj = row.get(translatedIndex);
	    				
	    				if(obj!=null && obj instanceof DateTime) {
	    				
	    					DateTime value = (DateTime)row.get(translatedIndex);
	    					
	    					if(range[0]==null || value.compareTo(range[0]) < 0) {
	    						range[0] = value;
	    					}
	    					
	    					if(range[1]==null || value.compareTo(range[1]) > 0) {
	    						range[1] = value;
	    					}
	    					
	    				} else {
	    					if(obj!=null && !(obj instanceof DateTime)) {
	    						System.err.println(String.format("{%s} row %d property %s has value of invalid type: '%s' (%s)", this.classIndices.get(row.getTableId()), row.getRowNumber(), col.getIdentifier(), obj, obj.getClass()));
	    					}
	    				}
    				}
    				
    			}
    			
    		}
    		
    	}
    }
    
	public DataSet<MatchableTableRow, MatchableTableColumn> getRecords() {
		return records;
	}

	public DataSet<MatchableTableColumn, MatchableTableColumn> getSchema() {
		return schema;
	}

	public DataSet<MatchableTable, MatchableTableColumn> getTables() {
		return tables;
	}
	
	public LinkedList<String> getProperties() {
		return properties;
	}

	public HashMap<String, Integer> getPropertyIds() {
		return propertyIds;
	}

	public Map<Integer, Map<Integer, Integer>> getPropertyIndices() {
		return propertyIndices;
	}

	public MatchableLodColumn getRdfsLabel() {
		return rdfsLabel;
	}
	
	public HashMap<Integer, Double> getClassWeight(){
		return classWeight;
	}
	/**
	 * @return the classIndices
	 */
	public Map<Integer, String> getClassIndices() {
		return classIndices;
	}
	/**
	 * @return the tableIds
	 */
	public Map<String, Integer> getClassIds() {
		return classToId;
	}
	
	/**
	 * @return the tables
	 */
	public HashMap<Integer, Integer> getTablesSize() {
		return sizePerTable;
	}
	
}
