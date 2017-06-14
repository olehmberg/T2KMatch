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
package de.uni_mannheim.informatik.dws.t2k.match.components;

import de.uni_mannheim.informatik.dws.t2k.match.data.MatchableTableColumn;
import de.uni_mannheim.informatik.dws.t2k.match.data.MatchableTableRow;
import de.uni_mannheim.informatik.dws.t2k.match.recordmapper.CorrespondenceWeightingRecordMapper;
import de.uni_mannheim.informatik.dws.t2k.match.recordmapper.GroupCorrespondencesRecordKeyMapper;
import de.uni_mannheim.informatik.dws.t2k.match.recordmapper.SumCorrespondenceSimilarityAggregator;
import de.uni_mannheim.informatik.dws.winter.model.Correspondence;
import de.uni_mannheim.informatik.dws.winter.model.Pair;
import de.uni_mannheim.informatik.dws.winter.processing.DataIterator;
import de.uni_mannheim.informatik.dws.winter.processing.Processable;

/**
 * Updates the schema correspondences from a previous iteration with the values from the current iteration.
 * 
 * @author Oliver Lehmberg (oli@dwslab.de)
 *
 */
public class UpdateSchemaCorrespondences {

	private Processable<Correspondence<MatchableTableColumn, MatchableTableRow>> schemaCorrespondences;
	public void setSchemaCorrespondences(
			Processable<Correspondence<MatchableTableColumn, MatchableTableRow>> schemaCorrespondences) {
		this.schemaCorrespondences = schemaCorrespondences;
	}
	
	private Processable<Correspondence<MatchableTableColumn, MatchableTableRow>> newSchemaCorrespondences;
	public void setNewSchemaCorrespondences(
			Processable<Correspondence<MatchableTableColumn, MatchableTableRow>> newSchemaCorrespondences) {
		this.newSchemaCorrespondences = newSchemaCorrespondences;
	}
	
	public UpdateSchemaCorrespondences() {
	}
	
	public Processable<Correspondence<MatchableTableColumn, MatchableTableRow>> run() {
		// multiply the similarity scores with the respective weights (old: 0.5; new: 0.5)
    	CorrespondenceWeightingRecordMapper weightDuplicateBased = new CorrespondenceWeightingRecordMapper(0.5);
    	schemaCorrespondences = schemaCorrespondences.transform(weightDuplicateBased);
    	
    	CorrespondenceWeightingRecordMapper weightLabelBased = new CorrespondenceWeightingRecordMapper(0.5);
    	newSchemaCorrespondences = newSchemaCorrespondences.transform(weightLabelBased);
    	
    	// combine the correspondences
    	Processable<Correspondence<MatchableTableColumn, MatchableTableRow>> combinresult = schemaCorrespondences.append(newSchemaCorrespondences);
    	
    	// group correspondences between the same column/property combination by summing up the weighted scores
    	GroupCorrespondencesRecordKeyMapper groupCorrespondences = new GroupCorrespondencesRecordKeyMapper();
    	SumCorrespondenceSimilarityAggregator sumSimilarity = new SumCorrespondenceSimilarityAggregator();
    	Processable<Pair<String, Correspondence<MatchableTableColumn, MatchableTableRow>>> sum = combinresult.aggregateRecords(groupCorrespondences, sumSimilarity);
    	
    	return sum.transform( 
			(Pair<String, Correspondence<MatchableTableColumn, MatchableTableRow>> record, 
				DataIterator<Correspondence<MatchableTableColumn, MatchableTableRow>> resultCollector)  ->
    		{
    			resultCollector.next(record.getSecond());	
			});
	}
}
