package de.uni_mannheim.informatik.dws.t2k.match;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

import au.com.bytecode.opencsv.CSVReader;
import au.com.bytecode.opencsv.CSVWriter;
import de.uni_mannheim.informatik.dws.t2k.match.data.MatchableTableRow;
import de.uni_mannheim.informatik.dws.t2k.match.data.WebTables;
import de.uni_mannheim.informatik.dws.winter.model.MatchingGoldStandard;

/**
 * 
 * Converts a gold standard from the original T2D format to the format used by {@link MatchingGoldStandard}
 * 
 * @author Sanikumar
 *
 */
public class GoldStandardConverter {

	private File readLocation;
	private File writeLocation;
	private WebTables web;

	public static void main(String[] args) throws IOException {
		
		String webLocation = args[0];
		String readGSLocation = args[1];
		String writeGSLocation = args[2];
		
    	if(readGSLocation != null && writeGSLocation != null){
    		
    		WebTables web = WebTables.loadWebTables(new File(webLocation), false, true, true);
    		
	    	//create an instances gold standard for evaluation purposes. If you create new instance gold standard then don not forget to load new instance gold standard
    		GoldStandardConverter gs = new GoldStandardConverter(readGSLocation, writeGSLocation, web);
	    	gs.convertOldGStoNewGS();
    	}
	}
	
	public GoldStandardConverter(String readLocation, String writeLocation, WebTables web) {
		this.readLocation = new File(readLocation);
		this.writeLocation = new File(writeLocation);
		this.web = web;
	}
	
	public void convertOldGStoNewGS() throws IOException{
		HashMap<String, Integer> tableIndices = web.getTableIndices();
		HashMap<Integer, Integer> keyIndices = web.getKeyIndices();
		
		CSVWriter csvWriter = new CSVWriter(new OutputStreamWriter(new FileOutputStream(writeLocation)));
		
		List<File> instanceGS = null;
    	
    	if(readLocation.isDirectory()) {
    		instanceGS = Arrays.asList(readLocation.listFiles());
    	} else {
    		instanceGS = Arrays.asList(new File[] { readLocation});
    	}
    	
    	for(MatchableTableRow trow : web.getRecords().get()){
    		SecondForLoop:
    		for(File f : instanceGS){
    			if(tableIndices.get(f.getName()) != null){
    				int tableID = tableIndices.get(f.getName());
    				if(trow.getTableId()==tableID){
    					if(trow.get(keyIndices.get(tableID))!=null){
    						CSVReader csvReader = new CSVReader(new InputStreamReader(new FileInputStream(f)));
    			    		String[] values;
    			    		while((values = csvReader.readNext()) != null){
    			    			String dbResourceLink = values[0];
    			    			String entity = values[1].replaceAll("\\s", "");
    			    			if(trow.get(keyIndices.get(tableID)).toString().replaceAll("\\s", "").equals(entity)){
    	    						String[] rowGS = {trow.getIdentifier(), dbResourceLink, "TRUE"};
    	    	    				csvWriter.writeNext(rowGS);
    	    	    				csvReader.close();
    	    	    				break SecondForLoop;
    	    	    			} else{
    	    	    				continue;
    	    	    			}
    			    		}
    			    		csvReader.close();
    					}
    				}
    			}
    		}
    	}
    	
    	csvWriter.close();
	}
	
}
