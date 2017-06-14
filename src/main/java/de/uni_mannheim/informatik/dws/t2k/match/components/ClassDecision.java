package de.uni_mannheim.informatik.dws.t2k.match.components;

import java.io.Serializable;

import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Map.Entry;

import de.uni_mannheim.informatik.dws.t2k.match.data.KnowledgeBase;
import de.uni_mannheim.informatik.dws.t2k.match.data.MatchableTableColumn;
import de.uni_mannheim.informatik.dws.t2k.match.data.MatchableTableRow;
import de.uni_mannheim.informatik.dws.winter.matching.MatchingEngine;
import de.uni_mannheim.informatik.dws.winter.model.Correspondence;
import de.uni_mannheim.informatik.dws.winter.model.Pair;
import de.uni_mannheim.informatik.dws.winter.processing.DataIterator;
import de.uni_mannheim.informatik.dws.winter.processing.Processable;
import de.uni_mannheim.informatik.dws.winter.processing.aggregators.DistributionAggregator;
import de.uni_mannheim.informatik.dws.winter.utils.Distribution;
import de.uni_mannheim.informatik.dws.winter.utils.query.Q;

/**
 * Component that performs the class decision.
 * 
 * @author Sanikumar
 * @author oliver
 */
public class ClassDecision implements Serializable{


	private static final long serialVersionUID = 1L;


	public ClassDecision() {

	}

	private Map<Integer, Set<String>> classDist = new HashMap<Integer, Set<String>>();
	private HashMap<Integer, Double> classWeight = new HashMap<Integer, Double>();
	
	
	public Map<Integer, Set<String>> runClassDecision(KnowledgeBase kb, Processable<Correspondence<MatchableTableRow, MatchableTableColumn>> instanceCorrespondences, MatchingEngine<MatchableTableRow, MatchableTableColumn> engine){

//    	calculate class weight for each class from Knowledge-Base
		classWeight = kb.getClassWeight();

		instanceCorrespondences = instanceCorrespondences.transform( 
			(Correspondence<MatchableTableRow, MatchableTableColumn> record,
				DataIterator<Correspondence<MatchableTableRow, MatchableTableColumn>> resultCollector) ->
			{
				if(classWeight.containsKey(record.getSecondRecord().getTableId())){
					record.setsimilarityScore(record.getSimilarityScore() + classWeight.get(record.getSecondRecord().getTableId()));
				}
				resultCollector.next(record);	
			});
		
//        choose top candidate for each instance by taking one to one mapping
//		TopKMatcher<MatchableTableRow, MatchableTableColumn> top1 = new TopKMatcher<>(proc, new TopKCorrespondencesRule<>(1, 0.0));
//		TopKMatch<MatchableTableRow, MatchableTableColumn> top1 = new TopKMatch<>(1, 0.0, proc);
//        Result<Correspondence<MatchableTableRow, MatchableTableColumn>> oneToOneInstanceCorrespondences = top1.getTopKMatch(instanceCorrespondences, proc);
//		Result<Correspondence<MatchableTableRow, MatchableTableColumn>> oneToOneInstanceCorrespondences = top1.runMatching(instanceCorrespondences);
		Processable<Correspondence<MatchableTableRow, MatchableTableColumn>> oneToOneInstanceCorrespondences = engine.getTopKInstanceCorrespondences(instanceCorrespondences, 1, 0.0);
        
//        count class distribution per table
         classDist = getClassDistribution(oneToOneInstanceCorrespondences, kb);
        
		return classDist;
		
	}
	
	public Map<Integer, Set<String>> getClassDistribution(Processable<Correspondence<MatchableTableRow, MatchableTableColumn>> correspondences, final KnowledgeBase kb) {
		
		Map<Integer, Set<String>> classesPerTable = new HashMap<Integer, Set<String>>();
		
		final Map<Integer, String> classIndices = kb.getClassIndices();

		Processable<Pair<Integer, Distribution<Integer>>> candidates = correspondences.aggregateRecords(
			// map each instance correspondence to its table id (= group by input table)
			(Correspondence<MatchableTableRow, MatchableTableColumn> record,
				DataIterator<Pair<Integer, Correspondence<MatchableTableRow, MatchableTableColumn>>> resultCollector) ->
			{
				resultCollector.next(new Pair<Integer, Correspondence<MatchableTableRow,MatchableTableColumn>>(record.getFirstRecord().getTableId(), record));
			}, 
			// count the frequency of each table id of the mapped records (= count how often each class was mapped to)
			new DistributionAggregator<Integer, Correspondence<MatchableTableRow,MatchableTableColumn>, Integer>() {

				private static final long serialVersionUID = 1L;

				@Override
				public Integer getInnerKey(Correspondence<MatchableTableRow, MatchableTableColumn> record) {
					return record.getSecondRecord().getTableId();
				}
			}
			);
		
		for(Pair<Integer, Distribution<Integer>> pair : candidates.get()) {
			HashSet<Integer> classes = new HashSet<Integer>();
			final Map<Integer, Double> nclassCounts = normalize(pair.getSecond());
			
//			prune all classes below similarity 0.5
			for(Map.Entry<Integer, Double> entry : nclassCounts.entrySet())
			{
				if(entry.getValue() >= 0.5){
					classes.add(entry.getKey());
				}
			}
			
			if(classesPerTable.isEmpty()){

				// if no class meets the similarity threshold, choose the top 5
				List<Map.Entry<Integer, Double>> classesSortedByFrequency = Q.sort(nclassCounts.entrySet(), new Comparator<Map.Entry<Integer, Double>>() {

					@Override
					public int compare(Entry<Integer, Double> o1, Entry<Integer, Double> o2) {
						return -Double.compare(o1.getValue(), o2.getValue());
					}});
				
				for(Map.Entry<Integer, Double> entry : classesSortedByFrequency) {
					
					classes.add(entry.getKey());
					if(classes.size() > 4)
						break;
				}
			}
			
			Set<String> selectedClasses = new HashSet<>();
			for(Integer i : classes) {
				selectedClasses.add(classIndices.get(i));
			}
			
			classesPerTable.put(pair.getFirst(), selectedClasses);
		}
		
		return classesPerTable;
	
	}
	
//	gives the normalized class counts
	public Map<Integer, Double> normalize(Distribution<Integer> classCounts){
		
		Map<Integer, Double> normalised = new HashMap<>();
		
		for(Integer key : classCounts.getElements()) {
			normalised.put(key, classCounts.getFrequency(key) / (double)classCounts.getMaxFrequency());
		}
		
		return normalised;
		
	}
}
