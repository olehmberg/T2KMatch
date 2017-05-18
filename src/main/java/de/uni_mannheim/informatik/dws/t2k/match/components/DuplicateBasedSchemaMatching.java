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

import java.util.Map;
import java.util.Set;

import de.uni_mannheim.informatik.dws.t2k.match.blocking.ClassAndTypeBasedSchemaBlocker;
import de.uni_mannheim.informatik.dws.t2k.match.comparators.MatchableTableRowComparator;
import de.uni_mannheim.informatik.dws.t2k.match.comparators.MatchableTableRowComparatorBasedOnSurfaceForms;
import de.uni_mannheim.informatik.dws.t2k.match.comparators.MatchableTableRowDateComparator;
import de.uni_mannheim.informatik.dws.t2k.match.data.KnowledgeBase;
import de.uni_mannheim.informatik.dws.t2k.match.data.MatchableTableColumn;
import de.uni_mannheim.informatik.dws.t2k.match.data.MatchableTableRow;
import de.uni_mannheim.informatik.dws.t2k.match.data.SurfaceForms;
import de.uni_mannheim.informatik.dws.t2k.match.data.WebTables;
import de.uni_mannheim.informatik.dws.t2k.match.rules.SchemaVotingRule;
import de.uni_mannheim.informatik.dws.t2k.match.rules.T2KVoting;
import de.uni_mannheim.informatik.dws.winter.matching.MatchingEngine;
import de.uni_mannheim.informatik.dws.winter.matching.aggregators.CorrespondenceAggregator;
import de.uni_mannheim.informatik.dws.winter.matching.aggregators.TopKVotesAggregator;
import de.uni_mannheim.informatik.dws.winter.model.Correspondence;
import de.uni_mannheim.informatik.dws.winter.preprocessing.datatypes.DataType;
import de.uni_mannheim.informatik.dws.winter.processing.Processable;
import de.uni_mannheim.informatik.dws.winter.similarity.SimilarityMeasure;
import de.uni_mannheim.informatik.dws.winter.similarity.date.WeightedDateSimilarity;
import de.uni_mannheim.informatik.dws.winter.similarity.numeric.DeviationSimilarity;
import de.uni_mannheim.informatik.dws.winter.similarity.string.GeneralisedStringJaccard;
import de.uni_mannheim.informatik.dws.winter.similarity.string.LevenshteinSimilarity;

/**
 * Component that runs the duplicate-based schema matching
 * 
 * @author Oliver Lehmberg (oli@dwslab.de)
 *
 */
public class DuplicateBasedSchemaMatching {

	private MatchingEngine<MatchableTableRow, MatchableTableColumn> matchingEngine;
	private WebTables web;
	private KnowledgeBase kb;
	private SurfaceForms surfaceForms;
	private Map<Integer, Set<String>> classesPerTable;
	private boolean matchKeys;
	
	private Processable<Correspondence<MatchableTableRow, MatchableTableColumn>> instanceCorrespondences;
	/**
	 * @param instanceCorrespondences the instanceCorrespondences to set
	 */
	public void setInstanceCorrespondences(
			Processable<Correspondence<MatchableTableRow, MatchableTableColumn>> instanceCorrespondences) {
		this.instanceCorrespondences = instanceCorrespondences;
	}
	
	private double valueSimilarityThreshold = 0.4;
	
	private double finalPropertySimilarityThreshold = 0.00;
	/**
	 * @param finalPropertySimilarityThreshold the finalPropertySimilarityThreshold to set
	 */
	public void setFinalPropertySimilarityThreshold(double finalPropertySimilarityThreshold) {
		this.finalPropertySimilarityThreshold = finalPropertySimilarityThreshold;
	}
	
	private SimilarityMeasure<String> stringSimilarity = new GeneralisedStringJaccard(new LevenshteinSimilarity(), 0.5, 0.5);
	
	private SimilarityMeasure<Double> numericSimilarity = new DeviationSimilarity();
	
	private WeightedDateSimilarity dateSimilarity = new WeightedDateSimilarity(1, 3, 5);
	
	private int numVotesPerValue = 0; 
	
	private int numCorrespondencesPerColumn = 3;
	
	private int numInstanceCandidates = 2;
	
	private double instanceCandidateThreshold = 0.5;
	
	public DuplicateBasedSchemaMatching(MatchingEngine<MatchableTableRow, MatchableTableColumn> matchingEngine, WebTables web, KnowledgeBase kb, SurfaceForms surfaceForms, Map<Integer, Set<String>> classesPerTable, Processable<Correspondence<MatchableTableRow, MatchableTableColumn>> instanceCorrespondences, boolean matchKeys) {
		this.matchingEngine = matchingEngine;
		this.web = web;
		this.kb = kb;
		this.surfaceForms = surfaceForms;
		this.classesPerTable = classesPerTable;
		this.instanceCorrespondences = instanceCorrespondences;
		this.matchKeys = matchKeys;
	}
	
	public Processable<Correspondence<MatchableTableColumn, MatchableTableRow>>  run() {
		// select the top k candidates for each row and apply min similarity
		Processable<Correspondence<MatchableTableRow, MatchableTableColumn>> bestCandidates = matchingEngine.getTopKInstanceCorrespondences(instanceCorrespondences, numInstanceCandidates, instanceCandidateThreshold);
		
    	// create the blocker
    	ClassAndTypeBasedSchemaBlocker classAndTypeBasedSchemaBlocker = new ClassAndTypeBasedSchemaBlocker(kb, classesPerTable);
    	
    	// run matching
    	SchemaVotingRule votingRule = new SchemaVotingRule(valueSimilarityThreshold);
    	votingRule.setComparatorForType(DataType.string, new MatchableTableRowComparatorBasedOnSurfaceForms(stringSimilarity, kb.getPropertyIndices(), valueSimilarityThreshold, surfaceForms));
    	votingRule.setComparatorForType(DataType.numeric, new MatchableTableRowComparator<>(numericSimilarity, kb.getPropertyIndices(), valueSimilarityThreshold));
    	votingRule.setComparatorForType(DataType.date, new MatchableTableRowDateComparator(dateSimilarity, kb.getPropertyIndices(), valueSimilarityThreshold));
    	if(!matchKeys) {
    		votingRule.setRdfsLabelId(kb.getRdfsLabel().getColumnIndex());
    	}
    	
    	// every value of the left-hand side has 2 votes for property correspondences (i.e. every lhs attribute creates up to two schema correspondences)
    	TopKVotesAggregator<MatchableTableColumn, MatchableTableRow> voteFilter = new TopKVotesAggregator<>(numVotesPerValue);
    	CorrespondenceAggregator<MatchableTableColumn, MatchableTableRow> voteAggregator = new T2KVoting(finalPropertySimilarityThreshold);

    	Processable<Correspondence<MatchableTableColumn, MatchableTableRow>> schemaCorrespondences = 
    			matchingEngine.runDuplicateBasedSchemaMatching(
    					web.getSchema(), 
    					kb.getSchema(), 
    					bestCandidates, 
    					votingRule, 
    					voteFilter, 
    					voteAggregator, 
    					classAndTypeBasedSchemaBlocker);
    	
    	// after aggregation, the best 3 schema correspondences for each attribute on the lhs are created
    	schemaCorrespondences = matchingEngine.getTopKSchemaCorrespondences(schemaCorrespondences, numCorrespondencesPerColumn, 0.0);

    	return schemaCorrespondences;
	}
	
}
