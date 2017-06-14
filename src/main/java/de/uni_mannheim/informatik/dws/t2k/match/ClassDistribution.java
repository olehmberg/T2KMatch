package de.uni_mannheim.informatik.dws.t2k.match;

import java.util.Collection;
import java.util.Map;

import de.uni_mannheim.informatik.dws.t2k.match.data.MatchableTableColumn;
import de.uni_mannheim.informatik.dws.t2k.match.data.MatchableTableRow;
import de.uni_mannheim.informatik.dws.winter.model.Correspondence;
import de.uni_mannheim.informatik.dws.winter.utils.Distribution;
import de.uni_mannheim.informatik.dws.winter.utils.query.Func;
import de.uni_mannheim.informatik.dws.winter.utils.query.Q;

/**
 * Utility class that generates a class distribution from correspondences.
 * 
 * @author Oliver Lehmberg (oli@dwslab.de)
 *
 */
public class ClassDistribution {
	
	public static Distribution<Integer> getClassDistribution(Collection<Correspondence<MatchableTableRow, MatchableTableColumn>> correspondences) {

    	Map<MatchableTableRow, Collection<Correspondence<MatchableTableRow, MatchableTableColumn>>> candidates = Q.group(correspondences, new Func<MatchableTableRow, Correspondence<MatchableTableRow, MatchableTableColumn>>() {

			@Override
			public MatchableTableRow invoke(Correspondence<MatchableTableRow, MatchableTableColumn> in) {
				return in.getFirstRecord();
			}
		});
		
    	Distribution<Integer> distribution = new Distribution<>();
		
		for(MatchableTableRow row : candidates.keySet()) {
			
			Collection<Correspondence<MatchableTableRow, MatchableTableColumn>> cands = candidates.get(row);
			
			Correspondence<MatchableTableRow, MatchableTableColumn> best = Q.max(cands, new Func<Double, Correspondence<MatchableTableRow, MatchableTableColumn>>() {

				@Override
				public Double invoke(Correspondence<MatchableTableRow, MatchableTableColumn> in) {
					return in.getSimilarityScore();
				}
			});
			
			distribution.add(best.getSecondRecord().getTableId());
		}
		
		return distribution;
	}
	
	public static Distribution<Integer> getAllCandidatesClassDistribution(Collection<Correspondence<MatchableTableRow, MatchableTableColumn>> correspondences) {

		Distribution<Integer> distribution = new Distribution<>();
		
		for(Correspondence<MatchableTableRow, MatchableTableColumn> cor : correspondences) {
			distribution.add(cor.getSecondRecord().getTableId());
		}
		
		return distribution;
	}
	
}
