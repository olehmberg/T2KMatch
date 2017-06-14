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

import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import de.uni_mannheim.informatik.dws.t2k.match.data.MatchableTableColumn;
import de.uni_mannheim.informatik.dws.t2k.match.data.MatchableTableRow;
import de.uni_mannheim.informatik.dws.t2k.match.data.WebTables;
import de.uni_mannheim.informatik.dws.winter.model.Correspondence;
import de.uni_mannheim.informatik.dws.winter.model.Pair;
import de.uni_mannheim.informatik.dws.winter.processing.DataIterator;
import de.uni_mannheim.informatik.dws.winter.processing.Function;
import de.uni_mannheim.informatik.dws.winter.processing.Processable;
import de.uni_mannheim.informatik.dws.winter.processing.RecordKeyValueMapper;
import de.uni_mannheim.informatik.dws.winter.processing.RecordMapper;
import de.uni_mannheim.informatik.dws.winter.processing.aggregators.CountAggregator;
import de.uni_mannheim.informatik.dws.winter.utils.query.Func;
import de.uni_mannheim.informatik.dws.winter.utils.query.Q;

/**
 * Calculates the ratio of mapped rows in each table and removes all correspondences if it is below the min. mapped ratio.
 * 
 * @author Oliver Lehmberg (oli@dwslab.de)
 *
 */
public class TableFiltering {

	private WebTables web;
	private Processable<Correspondence<MatchableTableRow, MatchableTableColumn>> instanceCorrespondences;
	private Processable<Correspondence<MatchableTableColumn, MatchableTableRow>> schemaCorrespondences;
	private Map<Integer, Set<String>> classesPerTable;
	
	/**
	 * @return the instanceCorrespondences
	 */
	public Processable<Correspondence<MatchableTableRow, MatchableTableColumn>> getInstanceCorrespondences() {
		return instanceCorrespondences;
	}
	/**
	 * @return the schemaCorrespondences
	 */
	public Processable<Correspondence<MatchableTableColumn, MatchableTableRow>> getSchemaCorrespondences() {
		return schemaCorrespondences;
	}
	/**
	 * @return the classesPerTable
	 */
	public Map<Integer, Set<String>> getClassesPerTable() {
		return classesPerTable;
	}
	
	private double minMappedRatio = 0.5;
	/**
	 * @param minMappedRatio the minMappedRatio to set
	 */
	public void setMinMappedRatio(double minMappedRatio) {
		this.minMappedRatio = minMappedRatio;
	}
	
	public TableFiltering(WebTables web,Processable<Correspondence<MatchableTableRow, MatchableTableColumn>> instanceCorrespondences, Map<Integer, Set<String>> classesPerTable, Processable<Correspondence<MatchableTableColumn, MatchableTableRow>> schemaCorrespondences) {
		this.web = web;
		this.instanceCorrespondences = instanceCorrespondences;
		this.classesPerTable = classesPerTable;
		this.schemaCorrespondences = schemaCorrespondences;
	}
	
	public void run() {
		// calculate the mapped ratio
		Processable<Pair<Integer, Double>> mappedRatio = calculateMappedRatio();
			
		// filter out tables
		filterClassCorrespondences(mappedRatio);
		
		// filter out instance correspondences
		filterInstanceCorrespondences(mappedRatio);

		// filter out schema correspondences
		filterSchemaCorrespondences(mappedRatio);
	}
	
	protected Processable<Pair<Integer, Double>> calculateMappedRatio() {
		// determine the number of records per table
		RecordKeyValueMapper<Integer, MatchableTableRow, MatchableTableRow> groupRecordByTableId = new RecordKeyValueMapper<Integer, MatchableTableRow, MatchableTableRow>() {
			
			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public void mapRecordToKey(MatchableTableRow record, DataIterator<Pair<Integer, MatchableTableRow>> resultCollector) {
				resultCollector.next(new Pair<Integer, MatchableTableRow>(record.getTableId(), record));
			}
		};
		Processable<Pair<Integer, Integer>> recordsPerTable = web.getRecords().aggregateRecords(groupRecordByTableId, new CountAggregator<Integer, MatchableTableRow>());
		
		// determine the number of correspondences per table
		RecordKeyValueMapper<Integer, Correspondence<MatchableTableRow, MatchableTableColumn>, Correspondence<MatchableTableRow, MatchableTableColumn>> groupCorrespondencesByTableId = new RecordKeyValueMapper<Integer, Correspondence<MatchableTableRow,MatchableTableColumn>, Correspondence<MatchableTableRow,MatchableTableColumn>>() {
			
			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public void mapRecordToKey(Correspondence<MatchableTableRow, MatchableTableColumn> record,
					DataIterator<Pair<Integer, Correspondence<MatchableTableRow, MatchableTableColumn>>> resultCollector) {
				resultCollector.next(new Pair<Integer, Correspondence<MatchableTableRow,MatchableTableColumn>>(record.getFirstRecord().getTableId(), record));
			}
		};
		Processable<Pair<Integer, Integer>> correspondencesPerTable = instanceCorrespondences.aggregateRecords(groupCorrespondencesByTableId, new CountAggregator<Integer, Correspondence<MatchableTableRow, MatchableTableColumn>>());
		
		// calculate the mapped ratio
		Function<Integer, Pair<Integer, Integer>> joinByTableId = new Function<Integer, Pair<Integer,Integer>>() {
			
			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public Integer execute(Pair<Integer, Integer> input) {
				return input.getFirst();
			}
		};
		Processable<Pair<Pair<Integer, Integer>, Pair<Integer, Integer>>> tableWithRecordsAndCorrespondences = recordsPerTable.join(correspondencesPerTable, joinByTableId, joinByTableId);
		
		RecordMapper<Pair<Pair<Integer, Integer>, Pair<Integer, Integer>>, Pair<Integer, Double>> calculateMappedRatio = new RecordMapper<Pair<Pair<Integer,Integer>,Pair<Integer,Integer>>, Pair<Integer,Double>>() {
			
			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public void mapRecord(Pair<Pair<Integer, Integer>, Pair<Integer, Integer>> record,
					DataIterator<Pair<Integer, Double>> resultCollector) {
				
				int tableId = record.getFirst().getFirst();
				int records = record.getFirst().getSecond();
				int correspondences = record.getSecond().getSecond();
				
				double ratio = correspondences / (double)records;
				
//				System.out.println(String.format("table: %d; records: %d; correspondences: %d; ratio: %.4f", tableId, records, correspondences, ratio));
				
				if(ratio>=minMappedRatio) {
					resultCollector.next(new Pair<Integer, Double>(tableId, ratio));
				}
				
			}
		};
		return tableWithRecordsAndCorrespondences.transform(calculateMappedRatio);
	}
	
	protected void filterClassCorrespondences(Processable<Pair<Integer, Double>> mappedRatio) {
		Collection<Integer> tablesToKeep = Q.project(mappedRatio.get(), new Func<Integer, Pair<Integer, Double>>() {

			@Override
			public Integer invoke(Pair<Integer, Double> in) {
				return in.getFirst();
			}
		});
		
		Iterator<Integer> tableIt = classesPerTable.keySet().iterator();
		while(tableIt.hasNext()) {
			if(!tablesToKeep.contains(tableIt.next())) {
				tableIt.remove();
			}
		}
	}
	
	protected void filterInstanceCorrespondences(Processable<Pair<Integer, Double>> mappedRatio) {
		Function<Integer, Correspondence<MatchableTableRow, MatchableTableColumn>> joinCorrespondenceByTableId = new Function<Integer, Correspondence<MatchableTableRow,MatchableTableColumn>>() {
			
			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public Integer execute(Correspondence<MatchableTableRow, MatchableTableColumn> input) {
				return input.getFirstRecord().getTableId();
			}
		};
		Function<Integer, Pair<Integer, Double>> joinMappedRatioByTableId = new Function<Integer, Pair<Integer,Double>>() {
			
			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public Integer execute(Pair<Integer, Double> input) {
				return input.getFirst();
			}
		};
		Processable<Pair<Correspondence<MatchableTableRow, MatchableTableColumn>, Pair<Integer, Double>>> filteredCorrespondences = instanceCorrespondences.join(mappedRatio, joinCorrespondenceByTableId, joinMappedRatioByTableId);
		
		RecordMapper<Pair<Correspondence<MatchableTableRow, MatchableTableColumn>, Pair<Integer, Double>>, Correspondence<MatchableTableRow, MatchableTableColumn>> getCorrespondences = new RecordMapper<Pair<Correspondence<MatchableTableRow,MatchableTableColumn>,Pair<Integer,Double>>, Correspondence<MatchableTableRow,MatchableTableColumn>>() {
			
			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public void mapRecord(Pair<Correspondence<MatchableTableRow, MatchableTableColumn>, Pair<Integer, Double>> record,
					DataIterator<Correspondence<MatchableTableRow, MatchableTableColumn>> resultCollector) {
				resultCollector.next(record.getFirst());
			}
		};
		instanceCorrespondences = filteredCorrespondences.transform(getCorrespondences);
	}
	
	protected void filterSchemaCorrespondences(Processable<Pair<Integer, Double>> mappedRatio) {
		Function<Integer, Correspondence<MatchableTableColumn, MatchableTableRow>> joinCorrespondenceByTableId = new Function<Integer, Correspondence<MatchableTableColumn, MatchableTableRow>>() {
			
			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public Integer execute(Correspondence<MatchableTableColumn, MatchableTableRow> input) {
				return input.getFirstRecord().getTableId();
			}
		};
		Function<Integer, Pair<Integer, Double>> joinMappedRatioByTableId = new Function<Integer, Pair<Integer,Double>>() {
			
			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public Integer execute(Pair<Integer, Double> input) {
				return input.getFirst();
			}
		};
		Processable<Pair<Correspondence<MatchableTableColumn, MatchableTableRow>, Pair<Integer, Double>>> filteredCorrespondences = schemaCorrespondences.join(mappedRatio, joinCorrespondenceByTableId, joinMappedRatioByTableId);
		
		RecordMapper<Pair<Correspondence<MatchableTableColumn, MatchableTableRow>, Pair<Integer, Double>>, Correspondence<MatchableTableColumn, MatchableTableRow>> getCorrespondences = new RecordMapper<Pair<Correspondence<MatchableTableColumn, MatchableTableRow>,Pair<Integer,Double>>, Correspondence<MatchableTableColumn, MatchableTableRow>>() {
			
			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public void mapRecord(Pair<Correspondence<MatchableTableColumn, MatchableTableRow>, Pair<Integer, Double>> record,
					DataIterator<Correspondence<MatchableTableColumn, MatchableTableRow>> resultCollector) {
				resultCollector.next(record.getFirst());
			}
		};
		schemaCorrespondences = filteredCorrespondences.transform(getCorrespondences);
	}

}
