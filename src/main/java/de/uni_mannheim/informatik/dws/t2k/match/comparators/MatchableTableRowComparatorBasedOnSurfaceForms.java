package de.uni_mannheim.informatik.dws.t2k.match.comparators;

import java.io.Serializable;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

import de.uni_mannheim.informatik.dws.t2k.match.data.MatchableTableRow;
import de.uni_mannheim.informatik.dws.t2k.match.data.SurfaceForms;
import de.uni_mannheim.informatik.dws.winter.similarity.SimilarityMeasure;

/**
 * 
 * Comarator that considers different surface forms for string comparisons.
 * 
 * @author Oliver Lehmberg (oli@dwslab.de)
 *
 */
public class MatchableTableRowComparatorBasedOnSurfaceForms extends MatchableTableRowComparator<String> implements Serializable {
	private static final long serialVersionUID = 6892158064887774907L;
	public MatchableTableRowComparatorBasedOnSurfaceForms() {
		
	}
	
	private SurfaceForms sf;
	
	public SurfaceForms getSf() {
		return sf;
	}

	private boolean removeBrackets = false;
//	bracketPattern is used remove a bracket including everything inside the bracket. for example, 'Republic Party (USA)' -> 'Republic Party'  
	private static final Pattern bracketsPattern = Pattern.compile("\\(.*\\)");
	
	public MatchableTableRowComparatorBasedOnSurfaceForms(SimilarityMeasure<String> similarity, Map<Integer, Map<Integer, Integer>> dbpPropertyIndices, double similarityThreshold, SurfaceForms sf) {
		super(similarity, dbpPropertyIndices, similarityThreshold);
		this.sf = sf;
	}

	public MatchableTableRowComparatorBasedOnSurfaceForms(SimilarityMeasure<String> similarity, Map<Integer, Map<Integer, Integer>> dbpPropertyIndices, double similarityThreshold, SurfaceForms sf, boolean removeBrackets) {
		super(similarity, dbpPropertyIndices, similarityThreshold);
		this.sf = sf;
		this.removeBrackets = removeBrackets;
	}
	
	public MatchableTableRowComparatorBasedOnSurfaceForms(SimilarityMeasure<String> similarity, Map<Integer, Map<Integer, Integer>> dbpPropertyIndices, boolean verbose) {
		super(similarity, dbpPropertyIndices, verbose);
	}
	
	/*
	 * 
	 */
	public boolean canCompareRecords(MatchableTableRow record1, MatchableTableRow record2, int firstColumnIndex, int secondColumnIndex) {
		Map<Integer, Integer> indexTranslation = getDbpPropertyIndices().get(record2.getTableId());
		if(indexTranslation==null) {
			return false;
		}
		
		Integer translatedIndex = indexTranslation.get(secondColumnIndex);
		if(translatedIndex==null) {
			return false;
		}
		
		return record1.getType(firstColumnIndex)==record2.getType(translatedIndex);
	}
	
	/*
	 * 
	 */
	public double compare(MatchableTableRow record1, MatchableTableRow record2, int firstColumnIndex, int secondColumnIndex) {
//		load surface-forms/redirects
		sf.loadIfRequired();
		
//		value to be compared from web table row. 'firstColumnIndex' tells which column value should be compared
		String value1 = (String)record1.get(firstColumnIndex);
		if(removeBrackets) {
			value1 = bracketsPattern.matcher(value1).replaceAll("");
		}
		
		Map<Integer, Integer> indexTranslation = getDbpPropertyIndices().get(record2.getTableId());
		if(indexTranslation==null) {
			System.err.println("Missing property index translation for table " + record2.getTableId());
		}
//		'secondColumnIndex' ('globalId' of dbpedia property) is used to get 'columnIndex' of dbpedia property in a respective table
		Integer translatedIndex = indexTranslation.get(secondColumnIndex);
		if(translatedIndex==null) {
			System.err.println(String.format("Missing property index translation for table %d property %d", record2.getTableId(), secondColumnIndex));
		}
		
//		value to be compared from dbpedia table row. 'translatedIndex' (columnIndex of dbpedia property in respective table) tells which column value should be compared
		String value2 = (String)record2.get(translatedIndex);
		
//		 list all possible value alternatives for the web table value (+redirect) and the dbpedia value (+surface forms)
		List<String> values2 = new LinkedList<>();
		
		values2.add(value2);
		for(String sForm : sf.getSurfaceForms(SurfaceForms.getNormalizedLabel(record2))){
			values2.add(sForm);
		}
		
//		remove the bracket from entity label
		if(removeBrackets) {
			List<String> newValues2 = new LinkedList<>();
			for(String s : values2) {
				newValues2.add(bracketsPattern.matcher(s).replaceAll(""));
			}
			values2 = newValues2;
		}
		
		
		double sim = Double.MIN_VALUE;
		
//		 use the max. similarity between any two values
		for(String v2 : values2) {
			double s = getSimilarity().calculate(value1, v2);
			
			sim = Math.max(s, sim);
		}
		
		if(isVerbose() && sim > 0.0) {
			System.out.println(String.format("(%.2f) '%s' <-> '%s'", sim, value1, value2));
		}
		
//		if the similarity satisfies threshold then return similarity, '0' otherwise
		return sim>=getSimilarityThreshold() ? sim : 0.0;
	}
}
