package de.uni_mannheim.informatik.dws.t2k.match.comparators;

import java.io.Serializable;
import java.util.Map;

import de.uni_mannheim.informatik.dws.t2k.match.data.MatchableTableColumn;
import de.uni_mannheim.informatik.dws.t2k.match.data.MatchableTableRow;
import de.uni_mannheim.informatik.dws.winter.similarity.SimilarityMeasure;

/**
 * 
 * Comparator for values with a specific data type.
 * 
 * @author Oliver Lehmberg (oli@dwslab.de)
 *
 * @param <Type>
 */
public class MatchableTableRowComparator<Type> implements Serializable { // extends Comparator<MatchableTableRow> {

	public MatchableTableRowComparator() {
		
	}
	private static final long serialVersionUID = 6892158064887774907L;
	private boolean verbose = false;
	private SimilarityMeasure<Type> similarity;
	protected Map<Integer, Map<Integer, Integer>> dbpPropertyIndices;
	private double similarityThreshold = 0.0;
	
	/**
	 * @return the verbose
	 */
	public boolean isVerbose() {
		return verbose;
	}
	
	/**
	 * @param verbose the verbose to set
	 */
	public void setVerbose(boolean verbose) {
		this.verbose = verbose;
	}
	
	public SimilarityMeasure<Type> getSimilarity() {
		return similarity;
	}
	
	public MatchableTableRowComparator(SimilarityMeasure<Type> similarity, Map<Integer, Map<Integer, Integer>> dbpPropertyIndices, double similarityThreshold) {
		this.similarity = similarity;
		this.dbpPropertyIndices = dbpPropertyIndices;
		this.similarityThreshold = similarityThreshold;
	}
	
	public MatchableTableRowComparator(SimilarityMeasure<Type> similarity, Map<Integer, Map<Integer, Integer>> dbpPropertyIndices, boolean verbose) {
		this.similarity = similarity;
		this.dbpPropertyIndices = dbpPropertyIndices;
		this.verbose = verbose;
	}

	public void setDbpPropertyIndices(
			Map<Integer, Map<Integer, Integer>> dbpPropertyIndices) {
		this.dbpPropertyIndices = dbpPropertyIndices;
	}
	
	public Map<Integer, Map<Integer, Integer>> getDbpPropertyIndices() {
		return dbpPropertyIndices;
	}
	
	/**
	 * @return the similarityThreshold
	 */
	public double getSimilarityThreshold() {
		return similarityThreshold;
	}
	/**
	 * @param similarityThreshold the similarityThreshold to set
	 */
	public void setSimilarityThreshold(double similarityThreshold) {
		this.similarityThreshold = similarityThreshold;
	}

	/**
	 * check whether two records can be compared
	 * 
	 * @param record1
	 *            the first record (must not be null)
	 * @param record2
	 *            the second record (must not be null)
	 * @param firstColumn
	 * 			  column of first records` value (must not be null)
	 * @param secondColumn
	 * 			  column of second records` value (must not be null) 
	 * @return the similarity of the records
	 */
	public boolean canCompareRecords(MatchableTableRow record1, MatchableTableRow record2, MatchableTableColumn firstColumn, MatchableTableColumn secondColumn) {
		Map<Integer, Integer> indexTranslation = dbpPropertyIndices.get(record2.getTableId());
		if(indexTranslation==null) {
			return false;
		}
		
		Integer translatedIndex = indexTranslation.get(secondColumn.getColumnIndex());
		if(translatedIndex==null) {
			return false;
		}
		
		return record1.getType(firstColumn.getColumnIndex())==record2.getType(translatedIndex) && record1.get(firstColumn.getColumnIndex())!=null && record2.get(translatedIndex)!=null;
	}
	
	@SuppressWarnings("unchecked")
	/**
	 * compares two records and returns a similarity value
	 * 
	 * @param record1
	 *            the first record (must not be null)
	 * @param record2
	 *            the second record (must not be null)
	 * @param firstColumnIndex
	 * 			  column index of first records` value (must not be null)
	 * @param secondColumnIndex
	 * 			  column index of second records` value (must not be null) 
	 * @return the similarity of the records
	 */
	public double compare(MatchableTableRow record1, MatchableTableRow record2, MatchableTableColumn firstColumn, MatchableTableColumn secondColumn) {
//		value to be compared from recor1. 'firstColumnIndex' tells which column value should be compared
		Type value1 = (Type)record1.get(firstColumn.getColumnIndex());
		
		Map<Integer, Integer> indexTranslation = dbpPropertyIndices.get(record2.getTableId());
		if(indexTranslation==null) {
			System.err.println("Missing property index translation for table " + record2.getTableId());
		}
		
//		'secondColumnIndex' ('globalId' of dbpedia property) is used to get 'columnIndex' of dbpedia property in a respective table
		Integer translatedIndex = indexTranslation.get(secondColumn.getColumnIndex());
		if(translatedIndex==null) {
			System.err.println(String.format("Missing property index translation for table %d property %d", record2.getTableId(), secondColumn.getColumnIndex()));
		}
		
//		value to be compared from record2. 'translatedIndex' (columnIndex of dbpedia property in respective table) tells which column value should be compared
		Type value2 = (Type)record2.get(translatedIndex);
		
//		calculate the similarity
		double sim = similarity.calculate(value1, value2);
		
		if(isVerbose() && sim > 0.0) {
			System.out.println(String.format("(%.2f) '%s' <-> '%s'", sim, value1, value2));
		}
		
//		if the similarity satisfies threshold then return the similarity, '0' otherwise
		return sim>=getSimilarityThreshold() ? sim : 0.0;
	}

}
