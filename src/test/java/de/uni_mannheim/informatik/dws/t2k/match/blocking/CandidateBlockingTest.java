/**
 * 
 */
package de.uni_mannheim.informatik.dws.t2k.match.blocking;

import org.joda.time.DateTime;

import de.uni_mannheim.informatik.dws.t2k.match.data.MatchableTableColumn;
import de.uni_mannheim.informatik.dws.t2k.match.data.MatchableTableRow;
import de.uni_mannheim.informatik.dws.winter.model.Correspondence;
import de.uni_mannheim.informatik.dws.winter.model.DataSet;
import de.uni_mannheim.informatik.dws.winter.model.HashedDataSet;
import de.uni_mannheim.informatik.dws.winter.preprocessing.datatypes.DataType;
import de.uni_mannheim.informatik.dws.winter.processing.Processable;
import de.uni_mannheim.informatik.dws.winter.processing.ProcessableCollection;
import junit.framework.TestCase;

/**
 * @author Sanikumar
 *
 */
public class CandidateBlockingTest extends TestCase {

	public void testRunBlocking(){
		DataSet<MatchableTableRow, MatchableTableColumn> dataset1 = new HashedDataSet<>();
		DataSet<MatchableTableRow, MatchableTableColumn> dataset2 = new HashedDataSet<>();
		
//		create web table rows
		MatchableTableRow wr1 = new MatchableTableRow("wa", new String[] { "republican", "Donald Trump" }, 0, new DataType[] { DataType.string, DataType.string });
		MatchableTableRow wr2 = new MatchableTableRow("wb", new String[] { "democratic", "Hillary Clinton" }, 0, new DataType[] { DataType.string, DataType.string });
		dataset1.add(wr1);
		dataset1.add(wr2);
		
//		create kb table rows
		MatchableTableRow kr1 = new MatchableTableRow("ka", new Object[] { "http://dbpedia.org/resource/Republic", "republican", DateTime.parse("1920") }, 0, new DataType[] { DataType.string, DataType.string, DataType.date });
		MatchableTableRow kr2 = new MatchableTableRow("kb", new Object[] { "http://dbpedia.org/resource/CDU_Party", "cdu party", DateTime.parse("1940") }, 0, new DataType[] { DataType.string, DataType.string, DataType.date });
		dataset2.add(kr1);
		dataset2.add(kr2);
		
//		create web table columns
		MatchableTableColumn wc1 = new MatchableTableColumn(0, 0, "label", DataType.string);
		new MatchableTableColumn(0, 1, "candidate", DataType.string);

//		create kb table columns
		new MatchableTableColumn(0, 0, "URI", DataType.link);
		MatchableTableColumn kc2 = new MatchableTableColumn(0, 1, "rdf-schema#label", DataType.string);
		new MatchableTableColumn(0, 2, "yearFounded", DataType.date);	

		
//		create schema correspondences between the web table and kb table
		Correspondence<MatchableTableColumn, MatchableTableRow> cor1 = new Correspondence<MatchableTableColumn, MatchableTableRow>(wc1, kc2, 0.5, null);
		Processable<Correspondence<MatchableTableColumn, MatchableTableRow>> schemaCorrespondences = new ProcessableCollection<>();
		schemaCorrespondences.add(cor1);
		
//		create instance correspondences
		Correspondence<MatchableTableRow, MatchableTableColumn> cor2 = new Correspondence<MatchableTableRow, MatchableTableColumn>(wr1, kr1, 1.0, null); 
		Processable<Correspondence<MatchableTableRow, MatchableTableColumn>> correspondences = new ProcessableCollection<>();
		correspondences.add(cor2);
		
//		create blocker
		CandidateBlocking blocker = new CandidateBlocking(correspondences);
		
		Processable<Correspondence<MatchableTableRow, MatchableTableColumn>> blockedPairs = blocker.runBlocking(dataset1, dataset2, Correspondence.simplify(schemaCorrespondences));
		
//		check for null pointer
		assertNotNull(blockedPairs);
		
//		check for size, as only the block pair between 'wa' and 'ka' will be created
		assertEquals(1, blockedPairs.size());
		
//		check for values
		for(Correspondence<MatchableTableRow, MatchableTableColumn> pair : blockedPairs.get()){
			assertEquals("wa", pair.getFirstRecord().getIdentifier());
			assertEquals("ka", pair.getSecondRecord().getIdentifier());
		}

	}
}
