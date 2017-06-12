package de.uni_mannheim.informatik.dws.t2k.match.rules;

import junit.framework.TestCase;
import de.uni_mannheim.informatik.dws.t2k.match.comparators.KeyValueComparatorBasedOnSurfaceForms;
import de.uni_mannheim.informatik.dws.t2k.match.data.MatchableTableColumn;
import de.uni_mannheim.informatik.dws.t2k.match.data.MatchableTableRow;
import de.uni_mannheim.informatik.dws.t2k.match.data.SurfaceForms;
import de.uni_mannheim.informatik.dws.winter.model.Correspondence;
import de.uni_mannheim.informatik.dws.winter.preprocessing.datatypes.DataType;
import de.uni_mannheim.informatik.dws.winter.processing.Processable;
import de.uni_mannheim.informatik.dws.winter.processing.ProcessableCollection;
import de.uni_mannheim.informatik.dws.winter.similarity.string.GeneralisedStringJaccard;
import de.uni_mannheim.informatik.dws.winter.similarity.string.LevenshteinSimilarity;

/**
 * @author Sanikumar
 *
 */
public class CandidateSelectionRuleTest extends TestCase {
	
	public void testApply(){
//		create table rows 
		MatchableTableRow r1 = new MatchableTableRow("a", new String[] { "republican" }, 0, new DataType[] { DataType.string });
		MatchableTableRow r2 = new MatchableTableRow("b", new String[] { "Republican Party (United States)" }, 0, new DataType[] { DataType.string });
		MatchableTableRow r3 = new MatchableTableRow("c", new String[] { "republican" }, 0, new DataType[] { DataType.string });
		MatchableTableRow r4 = new MatchableTableRow("d", new String[] { "democratic" }, 0, new DataType[] { DataType.string });
	
//		create table columns
		MatchableTableColumn c1 = new MatchableTableColumn(0, 0, "label", DataType.string);
		MatchableTableColumn c2 = new MatchableTableColumn(1, 0, "label", DataType.string);	
		MatchableTableColumn c3 = new MatchableTableColumn(0, 1, "author", DataType.string);
		MatchableTableColumn c4 = new MatchableTableColumn(1, 1, "book author", DataType.string);	
		
//		create schema correspondences
		Processable<Correspondence<MatchableTableColumn, MatchableTableRow>> schemaCorrespondence = new ProcessableCollection<>();
		Correspondence<MatchableTableColumn, MatchableTableRow> cor1 = new Correspondence<MatchableTableColumn, MatchableTableRow>(c1, c2, 1.0, null);
		Correspondence<MatchableTableColumn, MatchableTableRow> cor2 = new Correspondence<MatchableTableColumn, MatchableTableRow>(c3, c4, 0.5, null);
		schemaCorrespondence.add(cor1);
		schemaCorrespondence.add(cor2);
		
//		create the rule
		CandidateSelectionRule rule1 = new CandidateSelectionRule(0.25, 0);
		
//		create the comparator and add it to the rule
		KeyValueComparatorBasedOnSurfaceForms cmp1  = new KeyValueComparatorBasedOnSurfaceForms(new GeneralisedStringJaccard(new LevenshteinSimilarity(), 0.5, 0.25), 0, new SurfaceForms(null, null));
		rule1.setComparator(cmp1);
		assertNotNull(rule1.apply(r1, r2, Correspondence.toMatchable(schemaCorrespondence)));
		assertNull(rule1.apply(r3, r4, Correspondence.toMatchable(schemaCorrespondence)));

//		check for parameters
		assertNotNull(rule1.getComparator());
		assertNotNull(rule1.getRdfsLabelId());
		assertNotNull(rule1.getFinalThreshold());

//		create the rule
		CandidateSelectionRule rule2 = new CandidateSelectionRule(0.5, 0);
//		create the comparator and add it to the rule
		KeyValueComparatorBasedOnSurfaceForms cmp2  = new KeyValueComparatorBasedOnSurfaceForms(new GeneralisedStringJaccard(new LevenshteinSimilarity(), 0.5, 0.25), 0, new SurfaceForms(null, null));
		rule2.setComparator(cmp2);
		assertNotNull(rule2.apply(r1, r3, Correspondence.toMatchable(schemaCorrespondence)));
		assertNull(rule2.apply(r1, r2, Correspondence.toMatchable(schemaCorrespondence)));
		assertNull(rule2.apply(r3, r4, Correspondence.toMatchable(schemaCorrespondence)));
		
//		check for parameters
		assertNotNull(rule2.getComparator());
		assertNotNull(rule2.getRdfsLabelId());
		assertNotNull(rule2.getFinalThreshold());

	}
}
