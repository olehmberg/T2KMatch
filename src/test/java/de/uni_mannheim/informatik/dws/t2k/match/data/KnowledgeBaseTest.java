/**
 * 
 */
package de.uni_mannheim.informatik.dws.t2k.match.data;

import java.io.File;
import java.io.IOException;

import de.uni_mannheim.informatik.dws.winter.index.IIndex;
import de.uni_mannheim.informatik.dws.winter.index.io.DefaultIndex;
import junit.framework.TestCase;

/**
 * @author Sanikumar
 *
 */
public class KnowledgeBaseTest extends TestCase {
	/**
	 * Test method for {@link de.uni_mannheim.informatik.dws.t2k.match.data.KnowledgeBase#loadKnowledgeBase(java.io.File, de.uni_mannheim.informatik.dws.winter.index.IIndex, boolean, SurfaceForms)}
	 * @throws IOException 
	 */
	public void testLoadKnowledgeBase() throws IOException{
		KnowledgeBase kb = new KnowledgeBase();
		SurfaceForms sf = new SurfaceForms(new File("src\\test\\resources\\surfaceform\\SFs.txt"), new File("src\\test\\resources\\redirect\\redirects"));
		sf.loadIfRequired();
		IIndex index = new DefaultIndex("src\\test\\resources\\index\\");
		//first load DBpedia class Hierarchy
    	KnowledgeBase.loadClassHierarchy("src\\test\\resources\\ontology\\ontology");
    	assertNotNull(sf);
		kb = KnowledgeBase.loadKnowledgeBase(new File("src\\test\\resources\\kbtables\\"), index, sf);
		
//		check for number of records and schema
		assertEquals(10, kb.getRecords().size());
		assertEquals(4, kb.getSchema().size());	
		
//		check for table id using table file name
		assertEquals(0, (int) kb.getClassIds().get("kbtable1"));
		assertEquals(1, (int) kb.getClassIds().get("kbtable2"));
		assertNull(kb.getClassIds().get("kbtable3"));
		
//		check for dbpedia class using table id
		assertEquals("kbtable2", kb.getClassIndices().get(1));
		assertEquals("kbtable1", kb.getClassIndices().get(0));
		assertNull(kb.getClassIndices().get(2));
		
//		check for table size by table id
		assertEquals(5, (int) kb.getTablesSize().get(0));
		assertEquals(5, (int) kb.getTablesSize().get(1));
		assertNull(kb.getTablesSize().get(2));
		
//		check for class weight
		assertEquals(0.0, kb.getClassWeight().get(0));
		assertEquals(0.0, kb.getClassWeight().get(1));
		assertNull(kb.getClassWeight().get(5));
		
//		check for global property id using property uri
		assertEquals(2, (int) kb.getPropertyIds().get("http://dbpedia.org/ontology/PopulatedPlace/author"));
		assertEquals(3, (int) kb.getPropertyIds().get("http://dbpedia.org/ontology/year"));
		assertEquals(0, (int) kb.getPropertyIds().get("URI"));
		
//		check for property`s index (in a respective table) using table id and property`s global id
		assertEquals(2, (int) kb.getPropertyIndices().get(0).get(2));
		assertEquals(2, (int) kb.getPropertyIndices().get(1).get(3));
	}	
}
