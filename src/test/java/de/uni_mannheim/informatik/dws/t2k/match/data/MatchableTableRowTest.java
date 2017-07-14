package de.uni_mannheim.informatik.dws.t2k.match.data;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.temporal.ChronoField;
import java.util.Locale;

import de.uni_mannheim.informatik.dws.winter.preprocessing.datatypes.DataType;
import junit.framework.TestCase;

/**
 * @author Sanikumar
 *
 */
public class MatchableTableRowTest extends TestCase {
	/**
	 * Test method for {@link de.uni_mannheim.informatik.dws.t2k.match.data.MatchableTableRow}
	 */
	public void testMatchableTableRow(){
//		create table row
		DateTimeFormatter formatter = new DateTimeFormatterBuilder()
		        .appendPattern("yyyy-MM-dd")
		        .parseDefaulting(ChronoField.CLOCK_HOUR_OF_DAY, 0)
		        .parseDefaulting(ChronoField.MINUTE_OF_HOUR, 0)
		        .parseDefaulting(ChronoField.SECOND_OF_MINUTE, 0)
		        .toFormatter(Locale.ENGLISH);
		MatchableTableRow r1 = new MatchableTableRow("a", new String[] { "Republican Party (United States)" }, 0, new DataType[] { DataType.string });
		MatchableTableRow r2 = new MatchableTableRow("b", new Object[] { LocalDateTime.parse("1977-05-25", formatter) }, 0, new DataType[] { DataType.date });
		
//		check for values and null pointers
		assertNotNull(r1.getIdentifier());
		assertNotNull(r1.getTableId());
		assertNotNull(r1.getTypes());
		assertNotNull(r1.getValues());
		assertEquals(0, r1.getTableId());
		assertEquals("a", r1.getIdentifier());
		assertEquals(-1, r1.getRowNumber());
		assertNull(r1.getProvenance());
		
//		check for values and null pointers
		assertNotNull(r2.getIdentifier());
		assertNotNull(r2.getTableId());
		assertNotNull(r2.getTypes());
		assertNotNull(r2.getValues());
		assertEquals(0, r2.getTableId());
		assertEquals("b", r2.getIdentifier());
		assertEquals(-1,  r2.getRowNumber());
		assertNull(r2.getProvenance());
	}
}
