/** 
 *
 * Copyright (C) 2015 Data and Web Science Group, University of Mannheim, Germany (code@dwslab.de)
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 * 		http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */
package de.uni_mannheim.informatik.dws.t2k.index.dbpedia;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

import de.uni_mannheim.informatik.dws.winter.index.IIndex;
import de.uni_mannheim.informatik.dws.winter.index.io.DefaultIndex;

/**
 * 
 * Index look-up for entity labels.
 * 
 * @author Oliver Lehmberg (oli@dwslab.de)
 *
 */
public class KeyIndexLookup implements Serializable {
 
	private static final long serialVersionUID = 1L;

	public KeyIndexLookup() {
		
	}
	
	private DBPediaInstanceIndex idx;

	private IIndex index;
	private String indexLocation;
	private boolean verbose;
	private int numDoc = 50;
	private int dist = 2;
	
	public IIndex getIndex() {
        return index;
    }
	
	public String getIndexLocation() {
		return indexLocation;
	}

//	set the index location to search from
	public void setIndex(String indexLocation) {
        this.indexLocation = indexLocation;
    }
//    initialise index
	public void setIndex(IIndex index) {
		this.index = index;
		initialiseIndex();
	}
	
    public void setVerbose(boolean verbose) {
        this.verbose = verbose; 
    }
    
//    set the number of documents to retrieve from index
    public void setNumDocuments(int numDoc) {
        this.numDoc = numDoc;
    }
    
//    get the number of documents to retrieve from index
	public int getNumDoc() {
		return numDoc;
	}
    
//    max. edit distance to compare two string
    public void setMaxEditDistance(int dist) {
        this.dist = dist;
    }

//  max. edit distance to compare two string
	public int getDist() {
		return dist;
	}

    private void initialiseIndex() {
        idx = new DBPediaInstanceIndex(getIndex(), DBpediaIndexEntry.LABEL_FIELD);
        idx.setRemoveBrackets(true);
        idx.setVerbose(verbose);
        idx.setNumRetrievedDocsFromIndex(numDoc);
        idx.setMaxEditDistance(dist);
    }

    private Map<Integer, Set<String>> classesPerTable;
    
    /**
	 * @param classesPerTable the classesPerTable to set
	 */
	public void setClassesPerTable(Map<Integer, Set<String>> classesPerTable) {
		this.classesPerTable = classesPerTable;
	}
    
	public Collection<String> searchIndex(Object keyValue, int tableId) {
		if(idx==null) {
			this.index = new DefaultIndex(indexLocation);
	        initialiseIndex();
		}

		List<String> uniqueMatches = new ArrayList<>();

		Object lbl = keyValue;  
		if (lbl != null) {
			
			try {
				List<String> lbls = new ArrayList<>();
				lbls.add((String) lbl);
				
				// search the lucene index for the instance label
				List<DBpediaIndexEntry> matches = null;
				
				if(classesPerTable!=null) {
					matches = idx.searchMany(lbls,DBpediaIndexEntry.CLASS_LABEL_FIELD, classesPerTable.get(tableId));
				} else {
					matches = idx.searchMany(lbls);
				}
				
		        for (DBpediaIndexEntry e : matches) {		    
		        	if(!uniqueMatches.contains(e.getUri()))
		            	uniqueMatches.add(e.getUri());
		        }    
			} catch(Exception ex) {
				ex.printStackTrace();
			}
		}
		return uniqueMatches;
	}
	
	
}
