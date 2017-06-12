/** 
 *
 * Copyright (C) 2015 Data and Web Science Group, University of Mannheim, Germany (code@dwslab.de)
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 * 		http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */
package de.uni_mannheim.informatik.dws.t2k.match.rules;

import de.uni_mannheim.informatik.dws.t2k.match.data.MatchableTableColumn;
import de.uni_mannheim.informatik.dws.t2k.match.data.MatchableTableRow;
import de.uni_mannheim.informatik.dws.winter.matching.aggregators.VotingAggregator;
import de.uni_mannheim.informatik.dws.winter.model.Correspondence;
import de.uni_mannheim.informatik.dws.winter.model.Matchable;
import de.uni_mannheim.informatik.dws.winter.utils.query.Q;

/**
 * @author Oliver Lehmberg (oli@dwslab.de)
 *
 */
public class T2KVoting extends VotingAggregator<MatchableTableColumn, MatchableTableRow> {

	private static final long serialVersionUID = 1L;

	public T2KVoting(double finalThreshold) {
		super(true, finalThreshold);
	}
	
	/* (non-Javadoc)
	 * @see de.uni_mannheim.informatik.wdi.matching.matchers.VotingAggregator#getSimilarityScore(de.uni_mannheim.informatik.wdi.model.Correspondence)
	 */
	@Override
	protected double getSimilarityScore(Correspondence<MatchableTableColumn, MatchableTableRow> cor) {
		// cor represents a single vote
		// we weight the similarity with the similarity of the duplicate that produced this vote (stored in causalCorrespondences)
		
		Correspondence<MatchableTableRow, Matchable> cause = Q.firstOrDefault(cor.getCausalCorrespondences().get());
		
		if(cause==null) {
			// does not make sense, no duplicate created this vote
			return 0.0;
		} else {
			return cor.getSimilarityScore() * cause.getSimilarityScore();
		}
	}

}
