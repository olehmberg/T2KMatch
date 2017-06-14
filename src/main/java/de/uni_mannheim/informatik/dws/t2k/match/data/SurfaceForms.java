package de.uni_mannheim.informatik.dws.t2k.match.data;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.Serializable;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.Set;

import de.uni_mannheim.informatik.dws.winter.utils.query.Q;
import de.uni_mannheim.informatik.dws.winter.webtables.WebTablesStringNormalizer;

/**
 * 
 * Provides access to a dictionary of surface forms for string values.
 * 
 * @author Oliver Lehmberg (oli@dwslab.de)
 *
 */
public class SurfaceForms implements Serializable{
	
	
	/**
	 * loads surface forms in the form of HashMap<original_entity, <set of related surface forms>>, for example HashMap<abc, <cba, ABC, CBA>>
	 */
	private static final long serialVersionUID = 1L;
	
	private File locationSF, locationRD;
	private static int rdfsLabel = 1;
	
	public void setRdfsLabel(int rdfsLabel) {
		SurfaceForms.rdfsLabel = rdfsLabel;
	}

	//	entity and its surface forms 
	private static transient HashMap<String, String[]> surfaceForms = null;
	
	public SurfaceForms(File locationSF, File locationRD) {
		this.locationSF = locationSF;
		this.locationRD = locationRD;
		
		if(locationSF==null && locationRD==null) {
			surfaceForms = new HashMap<String, String[]>();
		}
	}
	
	public SurfaceForms(HashMap<String, String[]> surfaceForms){
		SurfaceForms.surfaceForms = surfaceForms;
	}

//	return surface forms (HashSet) if available
	public Set<String> getSurfaceForms(String forLabel) {
		if(surfaceForms==null || surfaceForms.containsKey(forLabel)) {
			return Q.toSet(surfaceForms.get(forLabel));
		} else {
			return new HashSet<>();
		}
	}

	protected void setSurfaceForms(HashMap<String, String[]> surfaceForms) {
		SurfaceForms.surfaceForms = surfaceForms;
	}
	
//	method to load surface forms and redirects when ever needed, 
//	makes use of thread locking mechanism to ensure that only one thread at a time get access to the instance (to prevent creating multiple instances) 
	public void loadIfRequired() {
		if(surfaceForms==null) {
			synchronized (this) {
				if(surfaceForms==null) {
					loadSurfaceForms();
				}
			}
		}
	}
	
	
//	method to load surface forms and redirects based on given location
	public void loadSurfaceForms(){
		
//		surface forms and redirects will be kept in same map as they both are used for the same purpose in the project.
		HashMap<String, LinkedList<String>> surfaceForms = new HashMap<String, LinkedList<String>>(); 
		
		BufferedReader br = null;
		
//		load surface forms if location is available
		if(locationSF!=null)
		{	
			try {
				System.out.println("Loading Surface Forms...");
				
				String sCurrentLine;
	
				br = new BufferedReader(new FileReader(locationSF));
	
				while ((sCurrentLine = br.readLine()) != null) {
					LinkedList<String> set = new LinkedList<String>();
					
//					because the file is 'Tab Delimited', we split the every line with 'tab' to separate entity from its surface forms.
					String[] tabDelimitedLine = sCurrentLine.split("\\t");
			        
					for(int i=1; i<tabDelimitedLine.length; i++){
			        		set.add(tabDelimitedLine[i].toString());
			        }
			        surfaceForms.put(tabDelimitedLine[0], set);
	
				}
				
				System.out.println("Loaded Surface Forms for " + surfaceForms.size() + " Resources.");
				
			} catch (IOException e) {
				e.printStackTrace();
			} finally {
				try {
					if (br != null)br.close();
				} catch (IOException ex) {
					ex.printStackTrace();
				}
			}
			
		} 
		
//		load redirects if location is available
		if(locationRD!=null){
		
			try {
				System.out.println("Loading Redirects...");
				
				int sfSize = surfaceForms.size();
				String sCurrentLine;
				br = new BufferedReader(new FileReader(locationRD));
	
				while ((sCurrentLine = br.readLine()) != null) {
					
//					because the file is 'Tab Delimited', we split the every line with 'tab' to separate entity from its redirects
					String[] tabDelimitedLine = sCurrentLine.split("\\t");
			    
					if(tabDelimitedLine.length < 2){
			        	continue;
			        }
			        else{
			        	if(tabDelimitedLine[0].isEmpty() || tabDelimitedLine[1].isEmpty())
			        		continue;
			        	else{
			        		if(surfaceForms.containsKey(tabDelimitedLine[0])){
			        			surfaceForms.get(tabDelimitedLine[0]).add(tabDelimitedLine[1]);
			        		} else{
			        			LinkedList<String> set = new LinkedList<String>();
				        		set.add(tabDelimitedLine[1]);
			        			surfaceForms.put(tabDelimitedLine[0], set);					        		
			        		}
			        	}
			        		
			        }
				}
				
				System.out.println("Loaded Redirecst for " + (surfaceForms.size() - sfSize) + " Resources.");
				
			} catch (IOException e) {
				e.printStackTrace();
			} finally {
				try {
					if (br != null)br.close();
				} catch (IOException ex) {
					ex.printStackTrace();
				}
			}	
		}
		
		HashMap<String, String[]> surfaceFormsArray = new HashMap<String, String[]>();
		
		for(String key : surfaceForms.keySet()) {
			Set<String> values = new HashSet<String>(surfaceForms.get(key));
			surfaceFormsArray.put(key, values.toArray(new String[values.size()]));
		}
		
		setSurfaceForms(surfaceFormsArray);
		
	}
	
//	method to normalize string value (label). this method is inherited from project 'Normalisation'
	public static String getNormalizedLabel(MatchableTableRow record){
		return WebTablesStringNormalizer.normaliseValue((String)record.get(rdfsLabel), false);
	}
	
}
