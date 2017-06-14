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

import de.uni_mannheim.informatik.dws.t2k.match.blocking.CandidateGenerationMultiKeyBlocker;
import de.uni_mannheim.informatik.dws.t2k.match.comparators.KeyValueComparatorBasedOnSurfaceForms;
import de.uni_mannheim.informatik.dws.t2k.match.data.KnowledgeBase;
import de.uni_mannheim.informatik.dws.t2k.match.data.MatchableTableColumn;
import de.uni_mannheim.informatik.dws.t2k.match.data.MatchableTableRow;
import de.uni_mannheim.informatik.dws.t2k.match.data.SurfaceForms;
import de.uni_mannheim.informatik.dws.t2k.match.data.WebTables;
import de.uni_mannheim.informatik.dws.t2k.match.rules.CandidateSelectionRule;
import de.uni_mannheim.informatik.dws.t2k.similarity.WebJaccardStringSimilarity;
import de.uni_mannheim.informatik.dws.winter.index.IIndex;
import de.uni_mannheim.informatik.dws.winter.matching.MatchingEngine;
import de.uni_mannheim.informatik.dws.winter.model.Correspondence;
import de.uni_mannheim.informatik.dws.winter.processing.Processable;

/**
 * 
 * Component for candidate refinement.
 * 
 * @author Oliver Lehmberg (oli@dwslab.de)
 *
 */
public class CandidateRefinement {

	private MatchingEngine<MatchableTableRow, MatchableTableColumn> matchingEngine;
	private boolean runsOnSpark;
	private IIndex index;
	private String indexLocation;
	private WebTables web;
	private KnowledgeBase kb;
	private SurfaceForms surfaceForms;
	private Processable<Correspondence<MatchableTableColumn, MatchableTableRow>> keyCorrespondences;
	private Map<Integer, Set<String>> classesPerTable;
	
	private int numCandidates = 100;
	
	private int maxEditDistance = 1;
	
	private double similarityThreshold = 0.7;
	
	public CandidateRefinement(MatchingEngine<MatchableTableRow, MatchableTableColumn> matchingEngine, boolean runsOnSpark, IIndex index, String indexLocation, WebTables web, KnowledgeBase kb, SurfaceForms surfaceForms, Processable<Correspondence<MatchableTableColumn, MatchableTableRow>> keyCorrespondences, Map<Integer, Set<String>> classesPerTable) {
		this.matchingEngine = matchingEngine;
		this.runsOnSpark = runsOnSpark;
		this.index = index;
		this.indexLocation = indexLocation;
		this.web = web;
		this.kb = kb;
		this.surfaceForms = surfaceForms;
		this.classesPerTable = classesPerTable;
		this.keyCorrespondences = keyCorrespondences; 
	}
	
	public Processable<Correspondence<MatchableTableRow, MatchableTableColumn>> run() {
    	// create the matching rule
    	CandidateSelectionRule candRule = new CandidateSelectionRule(similarityThreshold, kb.getRdfsLabel().getColumnIndex());
    	KeyValueComparatorBasedOnSurfaceForms keyComparator = new KeyValueComparatorBasedOnSurfaceForms(new WebJaccardStringSimilarity(), kb.getRdfsLabel().getColumnIndex(), surfaceForms);
    	candRule.setComparator(keyComparator);

    	// create the blocker
    	CandidateGenerationMultiKeyBlocker candidateGeneratingBlocker = null;
    	if(!runsOnSpark) {
    		// if we are using a single machine, pass the index
    		candidateGeneratingBlocker = new CandidateGenerationMultiKeyBlocker(index);
    	} else {
    		// if we are using spark, pass the location of the index; the workers will then load the index from there
    		candidateGeneratingBlocker = new CandidateGenerationMultiKeyBlocker(indexLocation);
    	}
    	// set the classes to consider
    	candidateGeneratingBlocker.setClassesPerTable(classesPerTable);
    	candidateGeneratingBlocker.setNumCandidates(numCandidates);
    	candidateGeneratingBlocker.setMaxEditDistance(maxEditDistance);
    	
    	// run candidate refinement
    	return matchingEngine.runIdentityResolution(web.getRecords(), kb.getRecords(), keyCorrespondences, candRule, candidateGeneratingBlocker);

	}
	
}
