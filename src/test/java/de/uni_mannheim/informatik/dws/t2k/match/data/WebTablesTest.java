package de.uni_mannheim.informatik.dws.t2k.match.data;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;

import de.uni_mannheim.informatik.dws.winter.model.Pair;
import junit.framework.TestCase;
/**
 * @author Sanikumar
 *
 */
public class WebTablesTest extends TestCase {
	
	public void testLoadWebTables() throws IOException, URISyntaxException{
		WebTables.setDoSerialise(false);
		WebTables wb = WebTables.loadWebTables(new File("src\\test\\resources\\webtables\\"), false, true, false);
			
//		check for number of records and schema
		assertEquals(10, wb.getRecords().size());
		assertEquals(7, wb.getSchema().size());			
		
//		check for table name using table id
		assertEquals("webtable1.csv", wb.getTableNames().get(0));
		assertEquals("webtable2.csv", wb.getTableNames().get(1));
		
//		check for table id sing table name
		assertEquals(0, (int) wb.getTableIndices().get("webtable1.csv"));
		assertEquals(1, (int) wb.getTableIndices().get("webtable2.csv"));
		assertEquals(wb.getKeyIndices().size(), wb.getKeys().size());
		
//		check for key column index in table using table id
		assertEquals(0, (int) wb.getKeyIndices().get(0));
		assertEquals(1, (int) wb.getKeyIndices().get(1));
		
//		check for column header using column identifier
		assertEquals("university", wb.getColumnHeaders().get("webtable2.csv~Col1"));
		assertEquals("name", wb.getColumnHeaders().get("webtable1.csv~Col0"));	
		assertEquals(wb.getColumnHeaders().size(), wb.getSchema().size());
		
//		check for table URl if table context is provided
		assertEquals("", wb.getTableURLs().get(0));
		assertEquals("", wb.getTableURLs().get(1));
		
//		check for key column (MatchableTableColumn object) using table id
		for(Pair<Integer, MatchableTableColumn> mc : wb.getKeys().get()){
			if(mc.getSecond().getTableId() == 1)
				assertEquals("webtable2.csv~Col1", mc.getSecond().getIdentifier());
			else
				assertEquals("webtable1.csv~Col0", mc.getSecond().getIdentifier());
		}
		
//		null pointer check for keepTablesInMemory
		assertNull(wb.getTablesById());
		
	}
}
