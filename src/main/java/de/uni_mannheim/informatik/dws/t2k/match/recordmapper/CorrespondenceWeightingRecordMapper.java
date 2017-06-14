package de.uni_mannheim.informatik.dws.t2k.match.recordmapper;

import de.uni_mannheim.informatik.dws.t2k.match.data.MatchableTableColumn;
import de.uni_mannheim.informatik.dws.t2k.match.data.MatchableTableRow;
import de.uni_mannheim.informatik.dws.winter.model.Correspondence;
import de.uni_mannheim.informatik.dws.winter.processing.DataIterator;
import de.uni_mannheim.informatik.dws.winter.processing.RecordMapper;

/**
 * 
 * Multiplies the similarity score of correspondences with the given weight
 * 
 * @author Oliver Lehmberg (oli@dwslab.de)
 *
 */
public class CorrespondenceWeightingRecordMapper implements RecordMapper<Correspondence<MatchableTableColumn, MatchableTableRow>, Correspondence<MatchableTableColumn, MatchableTableRow>>{

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	
	private double weight;

	public CorrespondenceWeightingRecordMapper(double weight) {
		super();
		this.weight = weight;
	}

	@Override
	public void mapRecord(
			Correspondence<MatchableTableColumn, MatchableTableRow> record,
			DataIterator<Correspondence<MatchableTableColumn, MatchableTableRow>> resultCollector) {
		record.setsimilarityScore(record.getSimilarityScore() * weight);
		resultCollector.next(record);
	}

}
