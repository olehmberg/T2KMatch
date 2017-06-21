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
import de.uni_mannheim.informatik.dws.winter.processing.RecordMapper;

/**
 * 
 * Component that combines the schema correspondences obtained from duplicate-based and label-based schema matching
 * 
 * @author Oliver Lehmberg (oli@dwslab.de)
 *
 */
public class CombineSchemaCorrespondences {

	private Processable<Correspondence<MatchableTableColumn, MatchableTableRow>> keyCorrespondences;
	
	private Processable<Correspondence<MatchableTableColumn, MatchableTableRow>> schemaCorrespondences;
	/**
	 * @param schemaCorrespondences the schemaCorrespondences to set
	 */
	public void setSchemaCorrespondences(
			Processable<Correspondence<MatchableTableColumn, MatchableTableRow>> schemaCorrespondences) {
		this.schemaCorrespondences = schemaCorrespondences;
	}
	
	private Processable<Correspondence<MatchableTableColumn, MatchableTableRow>> labelBasedSchemaCorrespondences;
	/**
	 * @param labelBasedSchemaCorrespondences the labelBasedSchemaCorrespondences to set
	 */
	public void setLabelBasedSchemaCorrespondences(
			Processable<Correspondence<MatchableTableColumn, MatchableTableRow>> labelBasedSchemaCorrespondences) {
		this.labelBasedSchemaCorrespondences = labelBasedSchemaCorrespondences;
	}
	
	public CombineSchemaCorrespondences(Processable<Correspondence<MatchableTableColumn, MatchableTableRow>> keyCorrespondences) {
		this.keyCorrespondences = keyCorrespondences;
	}
	
	private boolean verbose = false;
	/**
	 * @param verbose the verbose to set
	 */
	public void setVerbose(boolean verbose) {
		this.verbose = verbose;
	}
	/**
	 * @return the verbose
	 */
	public boolean isVerbose() {
		return verbose;
	}
	
	private double finalPropertySimilarityThreshold = 0.03;	
	
	public Processable<Correspondence<MatchableTableColumn, MatchableTableRow>> run() {
		// multiply the similarity scores with the respective weights (duplicate: 0.8; label: 0.2)
    	CorrespondenceWeightingRecordMapper weightDuplicateBased = new CorrespondenceWeightingRecordMapper(0.8);
    	schemaCorrespondences = schemaCorrespondences.map(weightDuplicateBased);
    	
    	CorrespondenceWeightingRecordMapper weightLabelBased = new CorrespondenceWeightingRecordMapper(0.2);
    	labelBasedSchemaCorrespondences = labelBasedSchemaCorrespondences.map(weightLabelBased);
    	
    	// combine the correspondences
    	Processable<Correspondence<MatchableTableColumn, MatchableTableRow>> combinresult = schemaCorrespondences.append(labelBasedSchemaCorrespondences);
    	
    	// group correspondences between the same column/property combination by summing up the weighted scores
    	GroupCorrespondencesRecordKeyMapper groupCorrespondences = new GroupCorrespondencesRecordKeyMapper();
    	SumCorrespondenceSimilarityAggregator sumSimilarity = new SumCorrespondenceSimilarityAggregator();
    	Processable<Pair<String, Correspondence<MatchableTableColumn, MatchableTableRow>>> sum = combinresult.aggregate(groupCorrespondences, sumSimilarity);
    	
    	if(isVerbose()) {
    		for(Pair<String, Correspondence<MatchableTableColumn, MatchableTableRow>> record : sum.get()) {
    			System.out.println(String.format("[%b] (%.8f) %s <-> %s", record.getSecond().getSimilarityScore()>=finalPropertySimilarityThreshold, record.getSecond().getSimilarityScore(), record.getSecond().getFirstRecord(), record.getSecond().getSecondRecord()));
    		}
    	}
    	
    	RecordMapper<Pair<String, Correspondence<MatchableTableColumn, MatchableTableRow>>, Correspondence<MatchableTableColumn, MatchableTableRow>> sumToCorrespondences = new RecordMapper<Pair<String,Correspondence<MatchableTableColumn,MatchableTableRow>>, Correspondence<MatchableTableColumn,MatchableTableRow>>() {
			
			private static final long serialVersionUID = 1L;

			@Override
			public void mapRecord(Pair<String, Correspondence<MatchableTableColumn, MatchableTableRow>> record,
					DataIterator<Correspondence<MatchableTableColumn, MatchableTableRow>> resultCollector) {
				if(record.getSecond().getSimilarityScore()>=finalPropertySimilarityThreshold) {
					resultCollector.next(record.getSecond());
				}
			}
		};
		schemaCorrespondences = sum.map(sumToCorrespondences);
			    	
    	schemaCorrespondences = schemaCorrespondences.append(keyCorrespondences);
    	
    	System.out.println(String.format("%d schema correspondences (including %d key correspondences)", schemaCorrespondences.size(), keyCorrespondences.size()));
    	
    	return schemaCorrespondences;
	}
	
}
