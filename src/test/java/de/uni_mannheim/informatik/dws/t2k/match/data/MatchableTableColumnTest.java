package de.uni_mannheim.informatik.dws.t2k.match.data;

import de.uni_mannheim.informatik.dws.winter.preprocessing.datatypes.DataType;
import junit.framework.TestCase;

/**
 * @author Sanikumar
 *
 */
public class MatchableTableColumnTest extends TestCase {
	/**
	 * Test method for {@link de.uni_mannheim.informatik.dws.t2k.match.data.MatchableTableColumn}
	 */

	public void testMatchableTableColumn(){
//		create table column
		MatchableTableColumn kc1 = new MatchableTableColumn(0, 0, "URI", DataType.link);
		MatchableTableColumn kc2 = new MatchableTableColumn(0, 1, "rdf-schema#label", DataType.string);
		MatchableTableColumn kc3 = new MatchableTableColumn(0, 2, "yearFounded", DataType.date);	
		
//		check for values and null pointers
		assertNotNull("", kc1.getIdentifier());
		assertEquals(0, kc1.getColumnIndex());
		assertEquals(0, kc1.getTableId());
		assertEquals(DataType.link, kc1.getType());
		assertNull(kc1.getProvenance());

//		check for values and null pointers
		assertNotNull("", kc2.getIdentifier());
		assertEquals(1, kc2.getColumnIndex());
		assertEquals(0, kc2.getTableId());
		assertEquals(DataType.string, kc2.getType());
		assertNull(kc2.getProvenance());
		
//		check for values and null pointers
		assertNotNull("", kc3.getIdentifier());
		assertEquals(2, kc3.getColumnIndex());
		assertEquals(0, kc3.getTableId());
		assertEquals(DataType.date, kc3.getType());
		assertNull(kc3.getProvenance());
	}
}
