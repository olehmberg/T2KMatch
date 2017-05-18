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
package de.uni_mannheim.informatik.dws.t2k.match.comparators;

import java.util.HashMap;
import java.util.Map;

import org.joda.time.DateTime;

import junit.framework.TestCase;
import de.uni_mannheim.informatik.dws.t2k.match.data.MatchableTableRow;
import de.uni_mannheim.informatik.dws.t2k.match.data.SurfaceForms;
import de.uni_mannheim.informatik.dws.winter.preprocessing.datatypes.DataType;
import de.uni_mannheim.informatik.dws.winter.similarity.string.GeneralisedStringJaccard;
import de.uni_mannheim.informatik.dws.winter.similarity.string.LevenshteinSimilarity;

/**
 * @author Oliver Lehmberg (oli@dwslab.de)
 *
 */
public class MatchableTableRowComparatorBasedOnSurfaceFormsTest extends TestCase {

	/**
	 * Test method for {@link de.uni_mannheim.informatik.dws.t2k.match.comparators.MatchableTableRowComparatorBasedOnSurfaceForms#compare(de.uni_mannheim.informatik.dws.t2k.match.data.MatchableTableRow, de.uni_mannheim.informatik.dws.t2k.match.data.MatchableTableRow, int, int)}.
	 */
	public void testCompare() {
//		create table rows
		MatchableTableRow r1 = new MatchableTableRow("a", new String[] { "republican" }, 0, new DataType[] { DataType.string });
		MatchableTableRow r2 = new MatchableTableRow("b", new String[] { "Republican Party (United States)" }, 0, new DataType[] { DataType.string });
		
//		create dbpedia properties indices
		Map<Integer, Map<Integer, Integer>> m = new HashMap<>();
		Map<Integer, Integer> map = new HashMap<>();
		map.put(0, 0);
		m.put(0, map);
		MatchableTableRowComparatorBasedOnSurfaceForms cmp = new MatchableTableRowComparatorBasedOnSurfaceForms(new GeneralisedStringJaccard(new LevenshteinSimilarity(), 0.5, 0.5), m, 0.5, new SurfaceForms(null, null), true);
		
//		check for parameters
		assertNotNull(cmp.getSf());
		assertNotNull(cmp.getSimilarity());
		assertNotNull(cmp.getSimilarityThreshold());
		assertNotNull(cmp.getDbpPropertyIndices());
		
//		check for similarity score
		assertEquals(50, (int)(cmp.compare(r1, r2, 0, 0)*100));
	}
	
	/**
	 * Test method for {@link de.uni_mannheim.informatik.dws.t2k.match.comparators.MatchableTableRowComparatorBasedOnSurfaceForms#canCompareRecords(MatchableTableRow, MatchableTableRow, int, int)}.
	 */
	public void testCanCompare(){
		MatchableTableRow r1 = new MatchableTableRow("a", new String[] { "republican" }, 0, new DataType[] { DataType.string });
		MatchableTableRow r2 = new MatchableTableRow("b", new String[] { "Republican Party (United States)" }, 0, new DataType[] { DataType.string });
		MatchableTableRow r3 = new MatchableTableRow("c", new Object[] { DateTime.parse("1977-05-25") }, 0, new DataType[] { DataType.date });
		MatchableTableRow r4 = new MatchableTableRow("d", new String[] { "democratic" }, 1, new DataType[]{ DataType.string });
		
//		create dbpedia properties indices
		Map<Integer, Map<Integer, Integer>> m = new HashMap<>();
		Map<Integer, Integer> map = new HashMap<>();
		map.put(0, 0);
		m.put(0, map);
		
//		create comparator
		MatchableTableRowComparatorBasedOnSurfaceForms cmp = new MatchableTableRowComparatorBasedOnSurfaceForms(new GeneralisedStringJaccard(new LevenshteinSimilarity(), 0.5, 0.5), m, 0.5, new SurfaceForms(null, null), true);
		
//		check for parameters
		assertNotNull(cmp.getSf());
		assertNotNull(cmp.getSimilarity());
		assertNotNull(cmp.getSimilarityThreshold());
		assertNotNull(cmp.getDbpPropertyIndices());
		
//		check whether two records can be compared
		assertTrue(cmp.canCompareRecords(r1, r2, 0, 0));
		assertFalse(cmp.canCompareRecords(r1, r3, 0, 0));
		assertFalse(cmp.canCompareRecords(r1, r4, 0, 0));
	}

}
