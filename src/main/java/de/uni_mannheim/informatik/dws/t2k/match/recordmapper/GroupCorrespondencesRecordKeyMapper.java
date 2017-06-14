package de.uni_mannheim.informatik.dws.t2k.match.recordmapper;

import de.uni_mannheim.informatik.dws.t2k.match.data.MatchableTableColumn;
import de.uni_mannheim.informatik.dws.t2k.match.data.MatchableTableRow;
import de.uni_mannheim.informatik.dws.winter.model.Correspondence;
import de.uni_mannheim.informatik.dws.winter.model.Pair;
import de.uni_mannheim.informatik.dws.winter.processing.DataIterator;
import de.uni_mannheim.informatik.dws.winter.processing.RecordKeyValueMapper;

/**
 * 
 * Groups correspondences by the identifiers of their two records (directed)
 * 
 * @author Oliver Lehmberg (oli@dwslab.de)
 *
 */
public class GroupCorrespondencesRecordKeyMapper implements RecordKeyValueMapper<String, Correspondence<MatchableTableColumn, MatchableTableRow>, Correspondence<MatchableTableColumn, MatchableTableRow>>{

	private static final long serialVersionUID = 1L;

	@Override
	public void mapRecordToKey(
			Correspondence<MatchableTableColumn, MatchableTableRow> record,
			DataIterator<Pair<String, Correspondence<MatchableTableColumn, MatchableTableRow>>> resultCollector) {
		resultCollector.next(new Pair<String, Correspondence<MatchableTableColumn,MatchableTableRow>>((record.getFirstRecord().getIdentifier() + record.getSecondRecord().getIdentifier()), record));
	}

}
