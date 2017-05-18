package de.uni_mannheim.informatik.dws.t2k.match.comparators;

import java.util.Map;

import org.joda.time.DateTime;

import de.uni_mannheim.informatik.dws.t2k.match.data.MatchableTableColumn;
import de.uni_mannheim.informatik.dws.t2k.match.data.MatchableTableRow;
import de.uni_mannheim.informatik.dws.winter.similarity.date.WeightedDateSimilarity;

/**
 * 
 * Specifc comparator for date values.
 * 
 * @author Oliver Lehmberg (oli@dwslab.de)
 *
 */
public class MatchableTableRowDateComparator extends MatchableTableRowComparator<DateTime> {

	private static final long serialVersionUID = -4859076819876528713L;

	public MatchableTableRowDateComparator() {
		
	}
	
	private WeightedDateSimilarity similarity;
	
	public MatchableTableRowDateComparator(WeightedDateSimilarity similarity, Map<Integer, Map<Integer, Integer>> dbpPropertyIndices, double similarityThreshold) {
		super(similarity, dbpPropertyIndices, similarityThreshold);
		this.similarity = similarity;
	}
	
	public MatchableTableRowDateComparator(WeightedDateSimilarity similarity, Map<Integer, Map<Integer, Integer>> dbpPropertyIndices, boolean verbose) {
		super(similarity, dbpPropertyIndices, verbose);
		this.similarity = similarity;
	}

	/**
	 * compares two records and returns a similarity value
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
	public double compare(MatchableTableRow record1, MatchableTableRow record2, MatchableTableColumn firstColumn, MatchableTableColumn secondColumn) {
//		value to be compared from recor1. 'firstColumnIndex' tells which column value should be compared
		DateTime value1 = (DateTime)record1.get(firstColumn.getColumnIndex());
		
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
		DateTime value2 = (DateTime)record2.get(translatedIndex);
		
		// normalise year difference with range from web table column
		DateTime min = (DateTime)firstColumn.getMin();
		DateTime max = (DateTime)firstColumn.getMax();
		similarity.setYearRange(Math.abs(max.getYear() - min.getYear()));
		
//		calculate the similarity
		double sim = similarity.calculate(value1, value2);
		
		if(isVerbose() && sim > 0.0) {
			System.out.println(String.format("(%.2f) '%s' <-> '%s'", sim, value1, value2));
		}
		
//		if the similarity satisfies threshold then return the similarity, '0' otherwise
		return sim>=getSimilarityThreshold() ? sim : 0.0;
	}

}
