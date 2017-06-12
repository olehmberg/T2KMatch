/**
 * 
 */
package de.uni_mannheim.informatik.dws.t2k.match.rules;

import java.util.HashMap;
import java.util.Map;

import org.joda.time.DateTime;

import de.uni_mannheim.informatik.dws.t2k.match.comparators.KeyValueComparatorBasedOnSurfaceForms;
import de.uni_mannheim.informatik.dws.t2k.match.comparators.MatchableTableRowComparator;
import de.uni_mannheim.informatik.dws.t2k.match.comparators.MatchableTableRowComparatorBasedOnSurfaceForms;
import de.uni_mannheim.informatik.dws.t2k.match.data.MatchableTableColumn;
import de.uni_mannheim.informatik.dws.t2k.match.data.MatchableTableRow;
import de.uni_mannheim.informatik.dws.t2k.match.data.SurfaceForms;
import de.uni_mannheim.informatik.dws.winter.model.Correspondence;
import de.uni_mannheim.informatik.dws.winter.preprocessing.datatypes.DataType;
import de.uni_mannheim.informatik.dws.winter.processing.Processable;
import de.uni_mannheim.informatik.dws.winter.processing.ProcessableCollection;
import de.uni_mannheim.informatik.dws.winter.similarity.date.WeightedDateSimilarity;
import de.uni_mannheim.informatik.dws.winter.similarity.string.GeneralisedStringJaccard;
import de.uni_mannheim.informatik.dws.winter.similarity.string.LevenshteinSimilarity;
import junit.framework.TestCase;

/**
 * @author Sanikumar
 *
 */
public class DataTypeDependentRecordMatchingRuleTest extends TestCase {
////	setup the data needed for the test
//	private KnowledgeBase kb = new KnowledgeBase();
//	private WebTables wb = new WebTables();
//	private ResultSet<Correspondence<MatchableTableColumn, MatchableTableRow>> schemaCorrespondence = new ResultSet<>();
//	
//	protected void setUp() throws IOException{
//		IIndex index = new DefaultIndex("src\\test\\resources\\index\\");
////		first load DBpedia class Hierarchy
//    	KnowledgeBase.loadClassHierarchy("src\\test\\resources\\ontology\\ontology");
//    	kb = KnowledgeBase.loadKnowledgeBase(new File("src\\test\\resources\\kbtables\\kbtable1.csv"), index, false, new SurfaceForms(null, null));
//    	
//    	wb = WebTables.loadWebTables(new File("src\\test\\resources\\webtables\\webtable1.csv"), false, true);
//    	
////    	create schema correspondences
//    	for(MatchableTableColumn mck : kb.getSchema().get()){
//			for(MatchableTableColumn mcw : wb.getSchema().get()){
//				if(mcw.getColumnIndex() != 1 || mcw.getColumnIndex() != 3 || mck.getColumnIndex() != 0){
//					schemaCorrespondence.add(new Correspondence<MatchableTableColumn, MatchableTableRow>(mcw, mck, 0.25, null));
//				}
//			}
//		}
//	}
	
	/**
	 * Test method for {@link de.uni_mannheim.informatik.dws.t2k.match.rules.DataTypeDependentRecordMatchingRule#apply(MatchableTableRow, MatchableTableRow, Processable)}
	 */
	public void testApply(){
		
//		create table rows for web table 
		MatchableTableRow wr1 = new MatchableTableRow("wa", new Object[] { "republican", DateTime.parse("1920") }, 0, new DataType[] { DataType.string, DataType.date });
		MatchableTableRow wr2 = new MatchableTableRow("wb", new Object[] { "democratic", DateTime.parse("1917") }, 0, new DataType[] { DataType.string, DataType.date });

//		create table rows for knowledge base
		MatchableTableRow kr1 = new MatchableTableRow("ka", new Object[] { "http://dbpedia.org/resource/Republic", "republican", DateTime.parse("1920") }, 0, new DataType[] { DataType.string, DataType.string, DataType.date });
		MatchableTableRow kr2 = new MatchableTableRow("kb", new Object[] { "http://dbpedia.org/resource/Republic_Party_(United States)", "Republican Party (United States)", DateTime.parse("1920") }, 0, new DataType[] { DataType.string, DataType.string, DataType.date });
		
//		create table column for web table
		MatchableTableColumn wc1 = new MatchableTableColumn(0, 0, "partyName", DataType.string);
		MatchableTableColumn wc2 = new MatchableTableColumn(0, 1, "year", DataType.date);	
		
//		create table column knowledge base
		@SuppressWarnings("unused")
		MatchableTableColumn kc1 = new MatchableTableColumn(0, 0, "URI", DataType.link);
		MatchableTableColumn kc2 = new MatchableTableColumn(0, 1, "rdf-schema#label", DataType.string);
		MatchableTableColumn kc3 = new MatchableTableColumn(0, 2, "yearFounded", DataType.date);	
		
//		create dbpedia properties id
		Map<Integer, Map<Integer, Integer>> m = new HashMap<>();
		Map<Integer, Integer> map = new HashMap<>();
		map.put(0, 0);
		map.put(1, 1);
		m.put(0, map);
		
//		create schema correspondence
		Correspondence<MatchableTableColumn, MatchableTableRow> cor1 = new Correspondence<MatchableTableColumn, MatchableTableRow>(wc1, kc2, 0.5, null);
		Correspondence<MatchableTableColumn, MatchableTableRow> cor2 = new Correspondence<MatchableTableColumn, MatchableTableRow>(wc2, kc3, 0.5, null);
		Processable<Correspondence<MatchableTableColumn, MatchableTableRow>> schemaCorrespondence = new ProcessableCollection<>();
		schemaCorrespondence.add(cor1);
		schemaCorrespondence.add(cor2);
		
//		create rule
		DataTypeDependentRecordMatchingRule rule1 = new DataTypeDependentRecordMatchingRule(0.5, 0);
		rule1.setComparatorForType(DataType.string, new MatchableTableRowComparatorBasedOnSurfaceForms(new GeneralisedStringJaccard(new LevenshteinSimilarity(), 0.5, 0.25), m, 0.25, new SurfaceForms(null, null)));
		rule1.setComparatorForType(DataType.date, new MatchableTableRowComparator<>(new WeightedDateSimilarity(1, 3, 5), m, 0.4));	
		rule1.setKeyValueComparator(new KeyValueComparatorBasedOnSurfaceForms(new GeneralisedStringJaccard(new LevenshteinSimilarity(), 0.5, 0.25), 1, new SurfaceForms(null, null)));
		
//		check for parameters
		assertNotNull(rule1.getFinalThreshold());
		assertNotNull(rule1.getComparators());
		assertNotNull(rule1.getKeyValueComparator());
		assertNotNull(rule1.getKeyValueWeight());
		assertNotNull(rule1.getRdfsLabelId());
		
//		check for return value of rule
		assertNotNull(rule1.apply(wr1, kr1, Correspondence.toMatchable(schemaCorrespondence)));
		assertNull(rule1.apply(wr1, kr2, Correspondence.toMatchable(schemaCorrespondence)));
		assertNull(rule1.apply(wr2, kr1, Correspondence.toMatchable(schemaCorrespondence)));
		
//		create rule
		DataTypeDependentRecordMatchingRule rule2 = new DataTypeDependentRecordMatchingRule(0.25, 0);
		rule2.setComparatorForType(DataType.string, new MatchableTableRowComparatorBasedOnSurfaceForms(new GeneralisedStringJaccard(new LevenshteinSimilarity(), 0.5, 0.25), m, 0.25, new SurfaceForms(null, null)));
		rule2.setComparatorForType(DataType.date, new MatchableTableRowComparator<>(new WeightedDateSimilarity(1, 3, 5), m, 0.4));	
		rule2.setKeyValueComparator(new KeyValueComparatorBasedOnSurfaceForms(new GeneralisedStringJaccard(new LevenshteinSimilarity(), 0.5, 0.25), 1, new SurfaceForms(null, null)));

//		check for parameters
		assertNotNull(rule2.getFinalThreshold());
		assertNotNull(rule2.getComparators());
		assertNotNull(rule2.getKeyValueComparator());
		assertNotNull(rule2.getKeyValueWeight());
		assertNotNull(rule2.getRdfsLabelId());
		
//		check for return value of rule
		assertNotNull(rule2.apply(wr1, kr1, Correspondence.toMatchable(schemaCorrespondence)));
		assertNotNull(rule2.apply(wr1, kr2, Correspondence.toMatchable(schemaCorrespondence)));
		assertNull(rule2.apply(wr2, kr1, Correspondence.toMatchable(schemaCorrespondence)));

	}
}
