package de.uni_mannheim.informatik.dws.t2k.match.rules;

import de.uni_mannheim.informatik.dws.t2k.match.data.MatchableTableColumn;
import de.uni_mannheim.informatik.dws.t2k.match.data.MatchableTableRow;
import de.uni_mannheim.informatik.dws.winter.matching.rules.Comparator;
import de.uni_mannheim.informatik.dws.winter.matching.rules.FilteringMatchingRule;
import de.uni_mannheim.informatik.dws.winter.model.Correspondence;
import de.uni_mannheim.informatik.dws.winter.model.SimpleCorrespondence;
import de.uni_mannheim.informatik.dws.winter.processing.Processable;

/**
 * 
 * Matching rule for the candidate selection.
 * 
 * @author Oliver Lehmberg (oli@dwslab.de)
 *
 */
public class CandidateSelectionRule extends FilteringMatchingRule<MatchableTableRow, MatchableTableColumn> {

	private static final long serialVersionUID = 1L;
	private Comparator<MatchableTableRow, MatchableTableColumn> comparator = null;

	private int rdfsLabelId;
    
	public Comparator<MatchableTableRow, MatchableTableColumn> getComparator() {
		return comparator;
	}

	public int getRdfsLabelId() {
		return rdfsLabelId;
	}

	/**
	 * @param comparator the comparator to set
	 */
	public void setComparator(Comparator<MatchableTableRow, MatchableTableColumn> comparator) {
		this.comparator = comparator;
	}
	
	public CandidateSelectionRule(double finalThreshold, int rdfsLabelId) {
		super(finalThreshold);
		this.rdfsLabelId = rdfsLabelId;
	}

	/*
	 * (non-Javadoc)
	 * @see de.uni_mannheim.informatik.dws.winter.matching.rules.FilteringMatchingRule#apply(de.uni_mannheim.informatik.dws.winter.model.Matchable, de.uni_mannheim.informatik.dws.winter.model.Matchable, de.uni_mannheim.informatik.dws.winter.processing.Processable)
	 */
	@Override
	public Correspondence<MatchableTableRow, MatchableTableColumn> apply(
			MatchableTableRow record1,
			MatchableTableRow record2,
			Processable<SimpleCorrespondence<MatchableTableColumn>> schemaCorrespondences) {
		
//		create schema correspondences between the key columns and rdfs:Label
		SimpleCorrespondence<MatchableTableColumn> keyCorrespondence = null;
		for(SimpleCorrespondence<MatchableTableColumn> cor :schemaCorrespondences.get()) {
			if(cor.getSecondRecord().getColumnIndex()==rdfsLabelId) {
				keyCorrespondence = cor;
				break;
			}
		}
		
//		calculate similarity
		//double sim = comparator.compare(record1, record2, keyCorrespondence);
		double sim = compare(record1, record2, keyCorrespondence);
		
//		if the similarity satisfies threshold then return the correspondence between two records, 'null' otherwise
		if(sim >= getFinalThreshold()) {
			return new Correspondence<MatchableTableRow, MatchableTableColumn>(record1, record2, sim, schemaCorrespondences);
		} else {
			return null;
		}
	}

	/* (non-Javadoc)
	 * @see de.uni_mannheim.informatik.wdi.matching.Comparator#compare(de.uni_mannheim.informatik.wdi.model.Matchable, de.uni_mannheim.informatik.wdi.model.Matchable, de.uni_mannheim.informatik.wdi.model.SimpleCorrespondence)
	 */
	@Override
	public double compare(MatchableTableRow record1, MatchableTableRow record2,
			SimpleCorrespondence<MatchableTableColumn> schemaCorrespondence) {
		return comparator.compare(record1, record2, schemaCorrespondence);
	}

}
