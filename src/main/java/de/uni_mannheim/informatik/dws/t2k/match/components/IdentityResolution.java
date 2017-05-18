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

import de.uni_mannheim.informatik.dws.t2k.match.blocking.CandidateBlocking;
import de.uni_mannheim.informatik.dws.t2k.match.comparators.KeyValueComparatorBasedOnSurfaceForms;
import de.uni_mannheim.informatik.dws.t2k.match.comparators.MatchableTableRowComparator;
import de.uni_mannheim.informatik.dws.t2k.match.comparators.MatchableTableRowComparatorBasedOnSurfaceForms;
import de.uni_mannheim.informatik.dws.t2k.match.comparators.MatchableTableRowDateComparator;
import de.uni_mannheim.informatik.dws.t2k.match.data.KnowledgeBase;
import de.uni_mannheim.informatik.dws.t2k.match.data.MatchableTableColumn;
import de.uni_mannheim.informatik.dws.t2k.match.data.MatchableTableRow;
import de.uni_mannheim.informatik.dws.t2k.match.data.SurfaceForms;
import de.uni_mannheim.informatik.dws.t2k.match.data.WebTables;
import de.uni_mannheim.informatik.dws.t2k.match.rules.DataTypeDependentRecordMatchingRule;
import de.uni_mannheim.informatik.dws.t2k.similarity.WebJaccardStringSimilarity;
import de.uni_mannheim.informatik.dws.winter.matching.MatchingEngine;
import de.uni_mannheim.informatik.dws.winter.model.Correspondence;
import de.uni_mannheim.informatik.dws.winter.preprocessing.datatypes.DataType;
import de.uni_mannheim.informatik.dws.winter.processing.Processable;
import de.uni_mannheim.informatik.dws.winter.similarity.SimilarityMeasure;
import de.uni_mannheim.informatik.dws.winter.similarity.date.WeightedDateSimilarity;
import de.uni_mannheim.informatik.dws.winter.similarity.numeric.DeviationSimilarity;
import de.uni_mannheim.informatik.dws.winter.similarity.string.GeneralisedStringJaccard;
import de.uni_mannheim.informatik.dws.winter.similarity.string.LevenshteinSimilarity;

/**
 * Component that runs the identity resolution.
 * 
 * @author Oliver Lehmberg (oli@dwslab.de)
 *
 */
public class IdentityResolution {

	private MatchingEngine<MatchableTableRow, MatchableTableColumn> matchingEngine;
	private WebTables web;
	private KnowledgeBase kb;
	private SurfaceForms sf;
	
	private Processable<Correspondence<MatchableTableRow, MatchableTableColumn>> instanceCorrespondences;
	/**
	 * @param instanceCorrespondences the instanceCorrespondences to set
	 */
	public void setInstanceCorrespondences(
			Processable<Correspondence<MatchableTableRow, MatchableTableColumn>> instanceCorrespondences) {
		this.instanceCorrespondences = instanceCorrespondences;
	}
	
	private Processable<Correspondence<MatchableTableColumn, MatchableTableRow>> schemaCorrespondences;
	/**
	 * @param schemaCorrespondences the schemaCorrespondences to set
	 */
	public void setSchemaCorrespondences(
			Processable<Correspondence<MatchableTableColumn, MatchableTableRow>> schemaCorrespondences) {
		this.schemaCorrespondences = schemaCorrespondences;
	}
	
	private SimilarityMeasure<String> stringSimilarity = new GeneralisedStringJaccard(new LevenshteinSimilarity(), 0.5, 0.5);
	private SimilarityMeasure<Double> numericSimilarity = new DeviationSimilarity();
	private WeightedDateSimilarity dateSimilarity = new WeightedDateSimilarity(1, 3, 5);
	
	private double valueSimilarityThreshold = 0.4;
	private double instanceCandidateThreshold = 0.1;
	private double keyValueWeight = 5.0;
	
	public IdentityResolution(MatchingEngine<MatchableTableRow, MatchableTableColumn> matchingEngine, WebTables web, KnowledgeBase kb, SurfaceForms sf) {
		this.matchingEngine = matchingEngine;
		this.web = web;
		this.kb = kb;
		this.sf = sf;
	}
	
	public Processable<Correspondence<MatchableTableRow, MatchableTableColumn>> run() {
    	// create blocking based on a set of instance correspondences
    	CandidateBlocking blocker = new CandidateBlocking(instanceCorrespondences);
   
    	// create matching rule
    	DataTypeDependentRecordMatchingRule instanceRule = new DataTypeDependentRecordMatchingRule(instanceCandidateThreshold, kb.getRdfsLabel().getColumnIndex());
    	instanceRule.setComparatorForType(DataType.string, new MatchableTableRowComparatorBasedOnSurfaceForms(stringSimilarity, kb.getPropertyIndices(), valueSimilarityThreshold, sf));
    	instanceRule.setComparatorForType(DataType.numeric, new MatchableTableRowComparator<>(numericSimilarity, kb.getPropertyIndices(), valueSimilarityThreshold));
    	instanceRule.setComparatorForType(DataType.date, new MatchableTableRowDateComparator(dateSimilarity, kb.getPropertyIndices(), valueSimilarityThreshold));
    	instanceRule.setKeyValueWeight(keyValueWeight);
    	KeyValueComparatorBasedOnSurfaceForms keyComparator = new KeyValueComparatorBasedOnSurfaceForms(new WebJaccardStringSimilarity(), kb.getRdfsLabel().getColumnIndex(), sf);
    	instanceRule.setKeyValueComparator(keyComparator);   	
    	
    	// run matching
    	return matchingEngine.runIdentityResolution(web.getRecords(), kb.getRecords(), schemaCorrespondences, instanceRule, blocker);
	}
	
}
