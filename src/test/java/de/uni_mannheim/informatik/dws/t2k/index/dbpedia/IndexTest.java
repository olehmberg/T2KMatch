package de.uni_mannheim.informatik.dws.t2k.index.dbpedia;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.temporal.ChronoField;
import java.util.Collection;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;

import de.uni_mannheim.informatik.dws.t2k.match.data.MatchableTableColumn;
import de.uni_mannheim.informatik.dws.t2k.match.data.MatchableTableRow;
import de.uni_mannheim.informatik.dws.t2k.match.data.SurfaceForms;
import de.uni_mannheim.informatik.dws.winter.index.IIndex;
import de.uni_mannheim.informatik.dws.winter.index.io.InMemoryIndex;
import de.uni_mannheim.informatik.dws.winter.model.DataSet;
import de.uni_mannheim.informatik.dws.winter.model.HashedDataSet;
import de.uni_mannheim.informatik.dws.winter.preprocessing.datatypes.DataType;
import junit.framework.TestCase;

/**
 * @author Sanikumar
 *
 */
public class IndexTest extends TestCase {
	/**
	 * Test method for {@link de.uni_mannheim.informatik.dws.t2k.index.dbpedia.DBpediaIndexer#indexInstances(de.uni_mannheim.informatik.dws.winter.index.IIndex, java.util.Collection, java.util.Map, de.uni_mannheim.informatik.dws.t2k.match.data.SurfaceForms)}.
	 * Test method for {@link de.uni_mannheim.informatik.dws.t2k.index.dbpedia.DBPediaInstanceIndex#searchMany(Collection)}.
	 */

	public void testIndexInstanceandSearchMany(){
		DataSet<MatchableTableRow, MatchableTableColumn> dataset = new HashedDataSet<>();
		
		DateTimeFormatter formatter = new DateTimeFormatterBuilder()
		        .appendPattern("yyyy")
		        .parseDefaulting(ChronoField.MONTH_OF_YEAR, 1)
		        .parseDefaulting(ChronoField.DAY_OF_MONTH, 1)
		        .parseDefaulting(ChronoField.CLOCK_HOUR_OF_DAY, 0)
		        .parseDefaulting(ChronoField.MINUTE_OF_HOUR, 0)
		        .parseDefaulting(ChronoField.SECOND_OF_MINUTE, 0)
		        .toFormatter(Locale.ENGLISH);
		
//		create kb table rows
		MatchableTableRow kr1 = new MatchableTableRow("ka", new Object[] { "http://dbpedia.org/resource/Republic", "republican", LocalDateTime.parse("1920", formatter) }, 0, new DataType[] { DataType.string, DataType.string, DataType.date });
		MatchableTableRow kr2 = new MatchableTableRow("kb", new Object[] { "http://dbpedia.org/resource/CDU_Party", "cdu party", LocalDateTime.parse("1940", formatter) }, 0, new DataType[] { DataType.string, DataType.string, DataType.date });
		MatchableTableRow kr3 = new MatchableTableRow("kc", new Object[] { "http://dbpedia.org/resource/Democratic", "democratic", LocalDateTime.parse("1922", formatter) }, 1, new DataType[] { DataType.string, DataType.string, DataType.date });
		MatchableTableRow kr4 = new MatchableTableRow("kd", new Object[] { "http://dbpedia.org/resource/Republic_Party", "republic party", LocalDateTime.parse("1920", formatter) }, 1, new DataType[] { DataType.string, DataType.string, DataType.date });
		dataset.add(kr1);
		dataset.add(kr2);
		dataset.add(kr3);
		dataset.add(kr4);
		
//		create class indices
		Map<Integer, String> classIndices = new HashMap<Integer, String>();
		classIndices.put(0, "kbtableOne");
		classIndices.put(1, "kbtableTwo");
		
//		create index
		IIndex index = new InMemoryIndex();
		DBpediaIndexer indexer = new DBpediaIndexer();
		indexer.indexInstances(index, dataset.get(), classIndices, new SurfaceForms(null, null));
		
//		check for values by searching index
		KeyIndexLookup lookup = new KeyIndexLookup();
		lookup.setIndex(index);
		
//		check for null pointers
		assertNotNull(lookup.getDist());
		assertNotNull(lookup.getNumDoc());
		assertNotNull(lookup.getIndex());
		
		Collection<String> lookupResult = lookup.searchIndex((Object) new String("democratic"), 0);
		
//		check for null pointer
		assertNotNull(lookupResult);
		
//		check for size and values
		assertEquals(1, lookupResult.size());
		for(String str : lookupResult){
			System.out.println(str);
			assertTrue(str.equals("http://dbpedia.org/resource/Democratic"));
		}
	}
}
