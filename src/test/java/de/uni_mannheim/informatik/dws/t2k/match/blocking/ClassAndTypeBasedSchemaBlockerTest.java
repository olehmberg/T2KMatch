/**
 * 
 */
package de.uni_mannheim.informatik.dws.t2k.match.blocking;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import de.uni_mannheim.informatik.dws.t2k.match.data.KnowledgeBase;
import de.uni_mannheim.informatik.dws.t2k.match.data.MatchableTableColumn;
import de.uni_mannheim.informatik.dws.t2k.match.data.MatchableTableRow;
import de.uni_mannheim.informatik.dws.t2k.match.data.SurfaceForms;
import de.uni_mannheim.informatik.dws.t2k.match.data.WebTables;
import de.uni_mannheim.informatik.dws.winter.index.IIndex;
import de.uni_mannheim.informatik.dws.winter.index.io.InMemoryIndex;
import de.uni_mannheim.informatik.dws.winter.model.Correspondence;
import de.uni_mannheim.informatik.dws.winter.processing.Processable;
import de.uni_mannheim.informatik.dws.winter.processing.ProcessableCollection;
import junit.framework.TestCase;

/**
 * @author Sanikumar
 *
 */
public class ClassAndTypeBasedSchemaBlockerTest extends TestCase {
	
	public void testRunBlocking() throws FileNotFoundException{
//		load web tables
		WebTables wb = new WebTables();
		wb = WebTables.loadWebTables(new File("src\\test\\resources\\webtables\\webtable1.csv"), false, true, false);
		
//		load kb table
		KnowledgeBase.setDoSerialise(false);
		IIndex index = new InMemoryIndex();
		KnowledgeBase kb = KnowledgeBase.loadKnowledgeBase(new File("src\\test\\resources\\kbtables\\"), index, new SurfaceForms(null, null));
		
//		create schema correspondences
		Processable<Correspondence<MatchableTableRow, MatchableTableColumn>> schemaCorrespondences = new ProcessableCollection<>();
		for(MatchableTableRow webCor : wb.getRecords().get()){
			for(MatchableTableRow kbCor : kb.getRecords().get()){
				String wLabel = webCor.getValues()[0].toString().toLowerCase();
				String kLabel = kbCor.getValues()[1].toString().toLowerCase();
				if(wLabel.equals(kLabel)){
					schemaCorrespondences.add(new Correspondence<MatchableTableRow, MatchableTableColumn>(webCor, kbCor, 1.0, null));
				}
			}
		}
		
		System.out.println(schemaCorrespondences.size());
		
//		create refinedClasses
		Map<Integer, Set<String>> refinedClasses = new HashMap<Integer, Set<String>>();
		Set<String> classes = new HashSet<String>();
		classes.add("kbtable1");
		refinedClasses.put(0, classes);
		
//		create blocker
		ClassAndTypeBasedSchemaBlocker blocker = new ClassAndTypeBasedSchemaBlocker(kb, refinedClasses);
		
//		check for null pointers
		assertNotNull(blocker.getKb());
		assertNotNull(blocker.getRefinedClasses());
		
		Processable<Correspondence<MatchableTableColumn, MatchableTableRow>> blockedPairs = blocker.runBlocking(wb.getSchema(), kb.getSchema(), Correspondence.toMatchable(schemaCorrespondences));

//		check for null pointer
		assertNotNull(blockedPairs);
		
//		check for size
		assertEquals(4, blockedPairs.size());
		
//		check for values
		for(Correspondence<MatchableTableColumn, MatchableTableRow> pair : blockedPairs.get()){
			System.out.println(pair.getFirstRecord().getIdentifier());
			System.out.println(pair.getSecondRecord().getIdentifier());
		}
	}
}
