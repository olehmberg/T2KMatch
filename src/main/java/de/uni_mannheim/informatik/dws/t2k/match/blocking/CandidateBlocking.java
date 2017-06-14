package de.uni_mannheim.informatik.dws.t2k.match.blocking;

import java.io.Serializable;

import de.uni_mannheim.informatik.dws.t2k.match.data.MatchableTableColumn;
import de.uni_mannheim.informatik.dws.t2k.match.data.MatchableTableRow;
import de.uni_mannheim.informatik.dws.winter.matching.blockers.AbstractBlocker;
import de.uni_mannheim.informatik.dws.winter.matching.blockers.Blocker;
import de.uni_mannheim.informatik.dws.winter.model.Correspondence;
import de.uni_mannheim.informatik.dws.winter.model.DataSet;
import de.uni_mannheim.informatik.dws.winter.model.Matchable;
import de.uni_mannheim.informatik.dws.winter.model.Pair;
import de.uni_mannheim.informatik.dws.winter.processing.DataIterator;
import de.uni_mannheim.informatik.dws.winter.processing.Processable;
import de.uni_mannheim.informatik.dws.winter.processing.ProcessableCollection;
import de.uni_mannheim.informatik.dws.winter.processing.RecordMapper;

/**
 * 
 * Blocker that uses a set of candidates to produce the blocked pairs.
 * A blocked pair will be one of the (record) correspondences that are provided in the constructor with the matching schema correspondences as causes. 
 * 
 * @author Oliver Lehmberg (oli@dwslab.de)
 *
 */
public class CandidateBlocking 
	extends AbstractBlocker<MatchableTableRow,MatchableTableRow,MatchableTableColumn> //<MatchableTableRow, MatchableTableColumn, MatchableTableColumn> 
	implements Blocker<MatchableTableRow, MatchableTableColumn, MatchableTableRow, MatchableTableColumn>, Serializable
{

	private static final long serialVersionUID = 1L;
	
	private Processable<Correspondence<MatchableTableRow, MatchableTableColumn>> correspondences;
	public CandidateBlocking(Processable<Correspondence<MatchableTableRow, MatchableTableColumn>> correspondences) {
		this.correspondences = correspondences;
	}
	
	/*
	 * (non-Javadoc)
	 * @see de.uni_mannheim.informatik.dws.winter.matching.blockers.CrossDataSetBlocker#runBlocking(de.uni_mannheim.informatik.dws.winter.model.DataSet, de.uni_mannheim.informatik.dws.winter.model.DataSet, de.uni_mannheim.informatik.dws.winter.processing.Processable)
	 */
	@Override
	public Processable<Correspondence<MatchableTableRow, MatchableTableColumn>> runBlocking(
			DataSet<MatchableTableRow, MatchableTableColumn> dataset1,
			DataSet<MatchableTableRow, MatchableTableColumn> dataset2,
			Processable<Correspondence<MatchableTableColumn, Matchable>> schemaCorrespondences) {
		
		// to get the matching schema correspondences, we have to join on both table ids
		// as we want a list of all schema correspondences, we use coGroup instead of join (which gives us collections for all objects with the same grouping key)
		Processable<Correspondence<MatchableTableRow, MatchableTableColumn>> result = correspondences.coGroup(schemaCorrespondences,
				(Correspondence<MatchableTableRow, MatchableTableColumn> input) -> input.getFirstRecord().getTableId(),
				(Correspondence<MatchableTableColumn, Matchable> input) -> input.getFirstRecord().getTableId(),
				new RecordMapper<Pair<Iterable<Correspondence<MatchableTableRow, MatchableTableColumn>>, Iterable<Correspondence<MatchableTableColumn, Matchable>>>, Correspondence<MatchableTableRow, MatchableTableColumn>>() {

					private static final long serialVersionUID = 1L;

					@Override
					public void mapRecord(
							Pair<Iterable<Correspondence<MatchableTableRow, MatchableTableColumn>>, Iterable<Correspondence<MatchableTableColumn, Matchable>>> record,
							DataIterator<Correspondence<MatchableTableRow, MatchableTableColumn>> resultCollector) {
						Processable<Correspondence<MatchableTableColumn, Matchable>> result2 = new ProcessableCollection<>();
						
						for(Correspondence<MatchableTableColumn, Matchable> ir : record.getSecond()) {
							result2.add(ir);
						}
						
						for(Correspondence<MatchableTableRow, MatchableTableColumn> ir : record.getFirst()) {
							resultCollector.next(new Correspondence<MatchableTableRow, MatchableTableColumn>(ir.getFirstRecord(), ir.getSecondRecord(), 1.0, result2));
						}
					}
					
				});
			
		calculatePerformance(dataset1, dataset2, result);
		
		return result;
		
	}



}
