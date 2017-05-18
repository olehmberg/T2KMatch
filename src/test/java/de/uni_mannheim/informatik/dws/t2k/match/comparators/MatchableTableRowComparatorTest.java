package de.uni_mannheim.informatik.dws.t2k.match.comparators;

import junit.framework.TestCase;

/**
 * @author Sanikumar
 */
public class MatchableTableRowComparatorTest extends TestCase {
	
	public void testCompare() {
////		create table rows
//		MatchableTableRow r1 = new MatchableTableRow("a", new String[] { "republican" }, 0, new DataType[] { DataType.string });
//		MatchableTableRow r2 = new MatchableTableRow("b", new String[] { "Republican Party (United States)" }, 0, new DataType[] { DataType.string });
//		
////		create dbpedia properties indices
//		Map<Integer, Map<Integer, Integer>> m = new HashMap<>();
//		Map<Integer, Integer> map = new HashMap<>();
//		map.put(0, 0);
//		m.put(0, map);
//		
////		create comparator
//		MatchableTableRowComparator<String> cmp1 = new MatchableTableRowComparator<String>(new GeneralisedStringJaccard(new LevenshteinSimilarity(), 0.5, 0.5), m, 0.5);
////		check for similarity value
////		assertEquals(0.0, cmp1.compare(r1, r2, 0, 0));
//		
////		create comparator
//		MatchableTableRowComparator<String> cmp2 = new MatchableTableRowComparator<String>(new GeneralisedStringJaccard(new LevenshteinSimilarity(), 0.5, 0.25), m, 0.25);
//		
////		check for parameters
//		assertNotNull(cmp1.getSimilarity());
//		assertNotNull(cmp1.getSimilarityThreshold());
//		assertNotNull(cmp1.getDbpPropertyIndices());
//		
//		assertNotNull(cmp2.getSimilarity());
//		assertNotNull(cmp2.getSimilarityThreshold());
//		assertNotNull(cmp2.getDbpPropertyIndices());
//		
//		check for similarity value
//		assertEquals(0.25, cmp2.compare(r1, r2, 0, 0));
	}

	
	public void testCanCompare(){
//		MatchableTableRow r1 = new MatchableTableRow("a", new String[] { "republican" }, 0, new DataType[] { DataType.string });
//		MatchableTableRow r2 = new MatchableTableRow("b", new String[] { "Republican Party (United States)" }, 0, new DataType[] { DataType.string });
//		MatchableTableRow r3 = new MatchableTableRow("c", new Object[] { DateTime.parse("1977-05-25") }, 0, new DataType[] { DataType.date });
//		MatchableTableRow r4 = new MatchableTableRow("d", new String[] { "democratic" }, 1, new DataType[]{ DataType.string });
//		
////		create dbpedia properties indices
//		Map<Integer, Map<Integer, Integer>> m = new HashMap<>();
//		Map<Integer, Integer> map = new HashMap<>();
//		map.put(0, 0);
//		m.put(0, map);
//		
////		create comparator
//		MatchableTableRowComparator<String> cmp = new MatchableTableRowComparator<String>(new GeneralisedStringJaccard(new LevenshteinSimilarity(), 0.5, 0.5), m, 0.5);
////		check for parameters
//		assertNotNull(cmp.getSimilarity());
//		assertNotNull(cmp.getSimilarityThreshold());
//		assertNotNull(cmp.getDbpPropertyIndices());
		
//		check whether two records can be compared
//		assertTrue(cmp.canCompareRecords(r1, r2, 0, 0));
//		assertFalse(cmp.canCompareRecords(r1, r3, 0, 0));
//		assertFalse(cmp.canCompareRecords(r1, r4, 0, 0));
	}
}
