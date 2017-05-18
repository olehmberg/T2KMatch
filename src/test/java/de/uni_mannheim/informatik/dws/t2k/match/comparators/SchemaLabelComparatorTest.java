package de.uni_mannheim.informatik.dws.t2k.match.comparators;

import junit.framework.TestCase;
import de.uni_mannheim.informatik.dws.t2k.match.data.MatchableTableColumn;
import de.uni_mannheim.informatik.dws.winter.preprocessing.datatypes.DataType;
import de.uni_mannheim.informatik.dws.winter.similarity.string.GeneralisedStringJaccard;
import de.uni_mannheim.informatik.dws.winter.similarity.string.LevenshteinSimilarity;

/**
 * @author Sanikumar
 *
 */
public class SchemaLabelComparatorTest extends TestCase {
	
	public void testCompare(){
//		create table columns
		MatchableTableColumn c1 = new MatchableTableColumn(0, 0, "author", DataType.string);
		MatchableTableColumn c2 = new MatchableTableColumn(1, 0, "book author", DataType.string);	
			
		SchemaLabelComparator cmp = new SchemaLabelComparator(new GeneralisedStringJaccard(new LevenshteinSimilarity(), 0.5, 0.5));
		
//		check for similarity value
		assertEquals(0.5, cmp.compare(c1, c2, null));
	}
}
