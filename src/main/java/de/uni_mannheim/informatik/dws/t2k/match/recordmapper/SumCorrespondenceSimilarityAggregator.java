package de.uni_mannheim.informatik.dws.t2k.match.recordmapper;

import de.uni_mannheim.informatik.dws.t2k.match.data.MatchableTableColumn;
import de.uni_mannheim.informatik.dws.t2k.match.data.MatchableTableRow;
import de.uni_mannheim.informatik.dws.winter.model.Correspondence;
import de.uni_mannheim.informatik.dws.winter.processing.DataAggregator;

/**
 * 
 * Sums up the similarity score of correspondences
 * 
 * @author Oliver Lehmberg (oli@dwslab.de)
 *
 */
public class SumCorrespondenceSimilarityAggregator implements DataAggregator<String, Correspondence<MatchableTableColumn,MatchableTableRow>, Correspondence<MatchableTableColumn,MatchableTableRow>>{

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	@Override
	public Correspondence<MatchableTableColumn, MatchableTableRow> aggregate(
			Correspondence<MatchableTableColumn, MatchableTableRow> previousResult,
			Correspondence<MatchableTableColumn, MatchableTableRow> record) {
		if(previousResult == null)
			return new Correspondence<MatchableTableColumn, MatchableTableRow>(record.getFirstRecord(), record.getSecondRecord(), record.getSimilarityScore(), record.getCausalCorrespondences());
		else{
			previousResult.setsimilarityScore(record.getSimilarityScore() + previousResult.getSimilarityScore());
			return previousResult;
		}
	}

	@Override
	public Correspondence<MatchableTableColumn, MatchableTableRow> initialise(
			String keyValue) {
		// TODO Auto-generated method stub
		return null;
	}

}
