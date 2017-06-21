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
package de.uni_mannheim.informatik.dws.t2k.match;

import java.util.HashMap;
import java.util.Map;

import de.uni_mannheim.informatik.dws.t2k.match.comparators.MatchableTableRowComparator;
import de.uni_mannheim.informatik.dws.t2k.match.data.ExtractedTriple;
import de.uni_mannheim.informatik.dws.t2k.match.data.KnowledgeBase;
import de.uni_mannheim.informatik.dws.t2k.match.data.MatchableTableColumn;
import de.uni_mannheim.informatik.dws.t2k.match.data.MatchableTableRow;
import de.uni_mannheim.informatik.dws.t2k.match.data.WebTables;
import de.uni_mannheim.informatik.dws.winter.model.Correspondence;
import de.uni_mannheim.informatik.dws.winter.model.Pair;
import de.uni_mannheim.informatik.dws.winter.preprocessing.datatypes.DataType;
import de.uni_mannheim.informatik.dws.winter.processing.DataIterator;
import de.uni_mannheim.informatik.dws.winter.processing.Function;
import de.uni_mannheim.informatik.dws.winter.processing.Processable;
import de.uni_mannheim.informatik.dws.winter.processing.RecordKeyValueMapper;
import de.uni_mannheim.informatik.dws.winter.processing.RecordMapper;
import de.uni_mannheim.informatik.dws.winter.processing.aggregators.CountAggregator;

/**
 * 
 * Counts the number of triples produces by the matching, including number of existing and number of new triples (no value in the KB).
 * 
 * @author Oliver Lehmberg (oli@dwslab.de)
 *
 */
public class TripleGenerator {
	
	private WebTables web;
	private KnowledgeBase kb;

	private int newTripleCount;
	private int existingTripleCount;
	private int correctTripleCount;
	private int incorrectTripleCount;
	
	private Map<DataType, MatchableTableRowComparator<?>> comparators = new HashMap<>();
	
	public void setComparatorForType(DataType type, MatchableTableRowComparator<?> comparator) {
		comparators.put(type, comparator);
	}
	
	/**
	 * @return the newTripleCount
	 */
	public int getNewTripleCount() {
		return newTripleCount;
	}
	/**
	 * @return the existingTripleCount
	 */
	public int getExistingTripleCount() {
		return existingTripleCount;
	}
	/**
	 * @return the correctTripleCount
	 */
	public int getCorrectTripleCount() {
		return correctTripleCount;
	}
	/**
	 * @return the incorrectTripleCount
	 */
	public int getIncorrectTripleCount() {
		return incorrectTripleCount;
	}
	
	public TripleGenerator(WebTables web, KnowledgeBase kb) {
		this.web = web;
		this.kb = kb;
	}
	
	public Processable<ExtractedTriple> run(Processable<Correspondence<MatchableTableRow, MatchableTableColumn>> instanceCorrespondences, Processable<Correspondence<MatchableTableColumn, MatchableTableRow>> schemaCorrespondences) {
	
		// group correspondences by table and then generate triples
		Function<Integer, Correspondence<MatchableTableRow, MatchableTableColumn>> groupInstanceCorByTable = new Function<Integer, Correspondence<MatchableTableRow,MatchableTableColumn>>() {
			
			private static final long serialVersionUID = 1L;

			@Override
			public Integer execute(Correspondence<MatchableTableRow, MatchableTableColumn> input) {
				return input.getFirstRecord().getTableId();
			}
		};
		Function<Integer, Correspondence<MatchableTableColumn, MatchableTableRow>> groupSchemaCorByTable = new Function<Integer, Correspondence<MatchableTableColumn,MatchableTableRow>>() {
			
			private static final long serialVersionUID = 1L;

			@Override
			public Integer execute(Correspondence<MatchableTableColumn, MatchableTableRow> input) {
				return input.getFirstRecord().getTableId();
			}
		};
		RecordMapper<Pair<Iterable<Correspondence<MatchableTableRow, MatchableTableColumn>>, Iterable<Correspondence<MatchableTableColumn, MatchableTableRow>>>, ExtractedTriple> resultMapper = new RecordMapper<Pair<Iterable<Correspondence<MatchableTableRow,MatchableTableColumn>>,Iterable<Correspondence<MatchableTableColumn,MatchableTableRow>>>, ExtractedTriple>() {
			
			private static final long serialVersionUID = 1L;

			@Override
			public void mapRecord(
					Pair<Iterable<Correspondence<MatchableTableRow, MatchableTableColumn>>, Iterable<Correspondence<MatchableTableColumn, MatchableTableRow>>> record,
					DataIterator<ExtractedTriple> resultCollector) {
				
				Correspondence<MatchableTableColumn, MatchableTableRow> key = null;
				
				// find the key correspondence
				for(Correspondence<MatchableTableColumn, MatchableTableRow> cor : record.getSecond()) {
					if(cor.getSecondRecord().getColumnIndex()==kb.getRdfsLabel().getColumnIndex()) {
						key = cor;
						break;
					}
				}
				
				if(key==null) {
					// we cannot create any triples without key
					return;
				}
				
				// for each mapped column
				for(Correspondence<MatchableTableColumn, MatchableTableRow> cor : record.getSecond()) {
					// that is not the key
					if(cor.getSecondRecord().getColumnIndex()!=kb.getRdfsLabel().getColumnIndex()) {
						
						// and for each mapped row
						for(Correspondence<MatchableTableRow, MatchableTableColumn> row : record.getFirst()) {
							
							// create a triple
							ExtractedTriple t = new ExtractedTriple();
							
							t.setSubjectURI(row.getSecondRecord().getIdentifier());
							t.setSubjectValue(row.getFirstRecord().get(key.getFirstRecord().getColumnIndex()).toString());
							t.setSubjectConfidence(row.getSimilarityScore());
							
							t.setPredicateURI(cor.getSecondRecord().getIdentifier());
							t.setPredicateValue(cor.getFirstRecord().getHeader());
							t.setPredicateConfidence(cor.getSimilarityScore());
							
							Object tableValue = row.getFirstRecord().get(cor.getFirstRecord().getColumnIndex());
							if(tableValue!=null) {
								t.setObjectValue(tableValue.toString());
								
								DataType type = row.getFirstRecord().getType(cor.getFirstRecord().getColumnIndex());
								t.setDataType(type);
								
								Integer kbColIndex = kb.getPropertyIndices().get(row.getSecondRecord().getTableId()).get(cor.getSecondRecord().getColumnIndex());
								Object kbValue = null;
								if(kbColIndex!=null) {
									kbValue = row.getSecondRecord().get(kbColIndex);
								} else {
									System.out.println(String.format("Mapped to invalid class/property combination: %s/%s", kb.getClassIndices().get(cor.getSecondRecord().getTableId()), cor.getSecondRecord().getHeader()));
								}
								if(kbValue!=null) {
									t.setObjectValueInKB(kbValue.toString());
									
									// evaluate via local-closed-world assumption
									MatchableTableRowComparator<?> comparator = comparators.get(type);
									double sim = comparator.compare(row.getFirstRecord(), row.getSecondRecord(), cor.getFirstRecord(), cor.getSecondRecord());
									t.setObjectValueMatchesKB(sim > 0.0);
									t.setEvaluatedSimilarity(sim);
								}
								
								t.setSourceURL(web.getTableURLs().get(row.getFirstRecord().getTableId()));
								t.setSourceTable(web.getTableNames().get(row.getFirstRecord().getTableId()));
								t.setSourceColumnIndex(cor.getFirstRecord().getColumnIndex());
								
								resultCollector.next(t);
							}
						}
						
					}
				}
				
				
			}
		};
		Processable<ExtractedTriple> triples = instanceCorrespondences.coGroup(schemaCorrespondences, groupInstanceCorByTable, groupSchemaCorByTable, resultMapper);
		
		// count the number of new and existing triples
		countNewTriples(triples);
		
		// count the number of correct triples according to LCWA
		countCorrectTriples(triples);
		
		return triples;
	}
	
	protected void countNewTriples(Processable<ExtractedTriple> triples) {
		RecordKeyValueMapper<Boolean, ExtractedTriple, ExtractedTriple> groupByIsNew = new RecordKeyValueMapper<Boolean, ExtractedTriple, ExtractedTriple>() {
			
			private static final long serialVersionUID = 1L;

			@Override
			public void mapRecordToKey(ExtractedTriple record, DataIterator<Pair<Boolean, ExtractedTriple>> resultCollector) {
				
				boolean isNew = record.getObjectValue()!=null && record.getObjectValueInKB()==null;
			
				resultCollector.next(new Pair<Boolean, ExtractedTriple>(isNew, record));
				
			}
		};
		Processable<Pair<Boolean, Integer>> counts = triples.aggregate(groupByIsNew, new CountAggregator<Boolean,ExtractedTriple>());
		
		for(Pair<Boolean, Integer> p : counts.get()) {
			if(p.getFirst()) {
				newTripleCount = p.getSecond();
			} else {
				existingTripleCount = p.getSecond();
			}
		}
	}
	
	protected void countCorrectTriples(Processable<ExtractedTriple> triples) {
		RecordKeyValueMapper<Boolean, ExtractedTriple, ExtractedTriple> groupByIsCorrect = new RecordKeyValueMapper<Boolean, ExtractedTriple, ExtractedTriple>() {
			
			private static final long serialVersionUID = 1L;

			@Override
			public void mapRecordToKey(ExtractedTriple record, DataIterator<Pair<Boolean, ExtractedTriple>> resultCollector) {
				
				boolean isCorrect = record.getObjectValue()!=null && record.getObjectValueInKB()!=null && record.getEvaluatedSimilarity() > 0.0;
			
				resultCollector.next(new Pair<Boolean, ExtractedTriple>(isCorrect, record));
				
			}
		};
		Processable<Pair<Boolean, Integer>> counts = triples.aggregate(groupByIsCorrect, new CountAggregator<Boolean,ExtractedTriple>());
		
		for(Pair<Boolean, Integer> p : counts.get()) {
			if(p.getFirst()) {
				correctTripleCount = p.getSecond();
			} else {
				incorrectTripleCount = p.getSecond();
			}
		}
	}
}
