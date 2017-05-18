package de.uni_mannheim.informatik.dws.t2k.match.comparators;

import junit.framework.TestCase;
import de.uni_mannheim.informatik.dws.t2k.match.data.MatchableTableColumn;
import de.uni_mannheim.informatik.dws.t2k.match.data.MatchableTableRow;
import de.uni_mannheim.informatik.dws.t2k.match.data.SurfaceForms;
import de.uni_mannheim.informatik.dws.winter.model.Correspondence;
import de.uni_mannheim.informatik.dws.winter.preprocessing.datatypes.DataType;
import de.uni_mannheim.informatik.dws.winter.similarity.string.GeneralisedStringJaccard;
import de.uni_mannheim.informatik.dws.winter.similarity.string.LevenshteinSimilarity;

/**
 * @author Sanikumar
 *
 */
public class KeyValueComparatorBasedOnSurfaceFormsTest extends TestCase {
	
	public void testcompare(){
//		create table rows 
		MatchableTableRow r1 = new MatchableTableRow("a", new String[] { "republican" }, 0, new DataType[] { DataType.string });
		MatchableTableRow r2 = new MatchableTableRow("b", new String[] { "Republican Party (United States)" }, 0, new DataType[] { DataType.string });
		MatchableTableRow r3 = new MatchableTableRow("c", new String[] { "republican" }, 0, new DataType[] { DataType.string });
		MatchableTableRow r4 = new MatchableTableRow("d", new String[] { "democratic" }, 0, new DataType[] { DataType.string });
	
//		create table column
		MatchableTableColumn c1 = new MatchableTableColumn(0, 0, "label", DataType.string);
		MatchableTableColumn c2 = new MatchableTableColumn(1, 0, "label", DataType.string);	
		
//		create schema correspondence
		Correspondence<MatchableTableColumn, MatchableTableRow> cor = new Correspondence<MatchableTableColumn, MatchableTableRow>(c1, c2, 1.0, null);
		
		KeyValueComparatorBasedOnSurfaceForms cmp  = new KeyValueComparatorBasedOnSurfaceForms(new GeneralisedStringJaccard(new LevenshteinSimilarity(), 0.5, 0.25), 0, new SurfaceForms(null, null));
		
//		check for different parameters of comparator
		assertNotNull(cmp.getSf());
		assertNotNull(cmp.getMeasure());
		
//		check for similarity values
		assertEquals(0.25, cmp.compare(r1, r2, cor));
		assertEquals(Double.MIN_VALUE, cmp.compare(r3, r4, cor));
		assertNotNull(cmp.compare(r3, r4, cor));
		
	}
}
