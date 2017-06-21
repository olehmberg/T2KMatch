package de.uni_mannheim.informatik.dws.t2k.match.blocking;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import de.uni_mannheim.informatik.dws.t2k.match.data.KnowledgeBase;
import de.uni_mannheim.informatik.dws.t2k.match.data.MatchableTableColumn;
import de.uni_mannheim.informatik.dws.t2k.match.data.MatchableTableRow;
import de.uni_mannheim.informatik.dws.winter.matching.blockers.AbstractBlocker;
import de.uni_mannheim.informatik.dws.winter.matching.blockers.Blocker;
import de.uni_mannheim.informatik.dws.winter.model.Correspondence;
import de.uni_mannheim.informatik.dws.winter.model.DataSet;
import de.uni_mannheim.informatik.dws.winter.model.Matchable;
import de.uni_mannheim.informatik.dws.winter.model.Pair;
import de.uni_mannheim.informatik.dws.winter.preprocessing.datatypes.DataType;
import de.uni_mannheim.informatik.dws.winter.processing.DataIterator;
import de.uni_mannheim.informatik.dws.winter.processing.Function;
import de.uni_mannheim.informatik.dws.winter.processing.Processable;
import de.uni_mannheim.informatik.dws.winter.processing.ProcessableCollection;
import de.uni_mannheim.informatik.dws.winter.processing.RecordMapper;
import de.uni_mannheim.informatik.dws.winter.utils.MapUtils;

/**
 * Blocks web table columns and DBpedia columns based on the refined classes and data types
 * 
 * @author Oliver Lehmberg (oli@dwslab.de)
 *
 */
public class ClassAndTypeBasedSchemaBlocker 
	extends AbstractBlocker<MatchableTableColumn,MatchableTableColumn,MatchableTableRow> //<MatchableTableColumn, MatchableTableColumn, MatchableTableRow>
	implements Blocker<MatchableTableColumn, MatchableTableColumn, MatchableTableColumn, MatchableTableRow>
{

	private Map<Integer, Set<String>> refinedClasses = new HashMap<Integer, Set<String>>();
	    
    public Map<Integer, Set<String>> getRefinedClasses() {
		return refinedClasses;
	}

	public ClassAndTypeBasedSchemaBlocker() {
		
	}	
		
	private KnowledgeBase kb;	
	
	public KnowledgeBase getKb() {
		return kb;
	}

	public ClassAndTypeBasedSchemaBlocker(KnowledgeBase kb, Map<Integer, Set<String>> refinedClasses) {
		super();
		this.kb = kb;
		this.refinedClasses = refinedClasses;
	}


	/*
	 * (non-Javadoc)
	 * @see de.uni_mannheim.informatik.dws.winter.matching.blockers.CrossDataSetBlocker#runBlocking(de.uni_mannheim.informatik.dws.winter.model.DataSet, de.uni_mannheim.informatik.dws.winter.model.DataSet, de.uni_mannheim.informatik.dws.winter.processing.Processable)
	 */
	@Override
	public Processable<Correspondence<MatchableTableColumn, MatchableTableRow>> runBlocking(
			DataSet<MatchableTableColumn, MatchableTableColumn> schema1,
			DataSet<MatchableTableColumn, MatchableTableColumn> schema2,
			final Processable<Correspondence<MatchableTableRow, Matchable>> instanceCorrespondences) {
		
		// schema1 - the web tables
		// schema2 - dbpedia
		// connection: refined classes - for each web table we have n class names
		// connection: class indices - for each dbpedia table id we have a class name 
		
		// first invert the direct of class indices, such that we can obtain a table id given a class name
		Map<String, Integer> nameToId = MapUtils.invert(kb.getClassIndices());

		
		// first translate class names to table ids and convert the map into a list of pairs
		// no need to use DataProcessingEngine as both variables are local
		Processable<Pair<Integer, Integer>> tablePairs = new ProcessableCollection<>();
		for(Integer webTableId : refinedClasses.keySet()) {
			
			Set<String> classesForTable = refinedClasses.get(webTableId);
			
			for(String className : classesForTable) {
				Pair<Integer, Integer> p = new Pair<Integer, Integer>(webTableId, nameToId.get(className));
				tablePairs.add(p);
			}
			
		}
		
		// we also have to determine in which classes each dbpedia property can occur
		// this map stores: dbpedia column id -> {table ids that have this column}
		final Map<Integer, Set<Integer>> classesPerColumnId = new HashMap<>();
		for(Integer tableId : kb.getPropertyIndices().keySet()) {
			
			// PropertyIndices maps a table id to a map of global property id -> local column index
			// here we are only interested in the global id
			Set<Integer> propertyIds = kb.getPropertyIndices().get(tableId).keySet();
			
			for(Integer columnId : propertyIds) {
				Set<Integer> tablesForColumnId = MapUtils.get(classesPerColumnId, columnId, new HashSet<Integer>());
				
				tablesForColumnId.add(tableId);
			}
		}
		
		//TODO the steps before this line should be done once in the driver program, so we don't have to transfer the knowledge base to the workers
		
		// now we join all web table columns with the just created pairs via the columns' table id and the first object of the pairs (which is the web table id)
		Function<Integer, MatchableTableColumn> tableColumnToTableId = new Function<Integer, MatchableTableColumn>() {

			private static final long serialVersionUID = 1L;

			@Override
			public Integer execute(MatchableTableColumn input) {
				return input.getTableId();
			}
		};

		Function<Integer, Pair<Integer, Integer>> pairToFirstObject = new Function<Integer, Pair<Integer, Integer>>() {

			private static final long serialVersionUID = 1L;

			@Override
			public Integer execute(Pair<Integer, Integer> input) {
				return input.getFirst();
			}
		};
		
		// this join results in: <web table column, <web table id, dbpedia table id>>
		Processable<Pair<MatchableTableColumn, Pair<Integer, Integer>>> tableColumnsWithClassIds = schema1.join(tablePairs, tableColumnToTableId, pairToFirstObject);
		
		// then we join the result with all dbpedia columns via the pairs' second object (which is the dbpedia table id) and the dbpedia columns' table id
		Function<Integer, Pair<MatchableTableColumn, Pair<Integer, Integer>>> tableColumnsWithClassIdsToClassId = new Function<Integer, Pair<MatchableTableColumn, Pair<Integer, Integer>>>() {

			private static final long serialVersionUID = 1L;

			@Override
			public Integer execute(Pair<MatchableTableColumn, Pair<Integer, Integer>> input) {
				// input.getSecond() returns the pair that we created in the beginning
				// so that pair's second is the dbpedia table id
				return input.getSecond().getSecond();
			}
		};
		
		
		// for dbpedia columns we have to consider which properties exist for which class (a property can exist for multiple classes)
		// to make it work, we create pairs of <dbpedia table id, dbpedia column> for all tables where a property exists
		RecordMapper<MatchableTableColumn, Pair<Integer, MatchableTableColumn>> dbpediaColumnToTableIdMapper = new RecordMapper<MatchableTableColumn, Pair<Integer,MatchableTableColumn>>() {

			private static final long serialVersionUID = 1L;

			@Override
			public void mapRecord(MatchableTableColumn record,
					DataIterator<Pair<Integer, MatchableTableColumn>> resultCollector) {
				
				for(Integer tableId : classesPerColumnId.get(record.getColumnIndex())) {
					Pair<Integer, MatchableTableColumn> tableWithColumn = new Pair<Integer, MatchableTableColumn>(tableId, record);
					
					resultCollector.next(tableWithColumn);
				}
				
			}
		};
		Processable<Pair<Integer, MatchableTableColumn>> dbpediaColumnsForAllTables = schema2.map(dbpediaColumnToTableIdMapper);
		
		Function<Integer, Pair<Integer, MatchableTableColumn>> dbpediaColumnToTableId = new Function<Integer, Pair<Integer, MatchableTableColumn>>() {

			private static final long serialVersionUID = 1L;

			@Override
			public Integer execute(Pair<Integer, MatchableTableColumn> input) {
				return input.getFirst();
			}
		};
		
		// this join resuts in: <<web table column, <web table id, dbpedia table id>>, <dbpedia table id, dbpedia column>> 
		Processable<Pair<Pair<MatchableTableColumn, Pair<Integer, Integer>>, Pair<Integer, MatchableTableColumn>>> columnsJoinedViaRefinedClasses = tableColumnsWithClassIds.join(dbpediaColumnsForAllTables, tableColumnsWithClassIdsToClassId, dbpediaColumnToTableId);
			
		// we also filter out pairs of columns with non-matching data types 
		// while doing this, we simplify the data structure by removing the pair of table ids
		RecordMapper<Pair<Pair<MatchableTableColumn, Pair<Integer, Integer>>, Pair<Integer, MatchableTableColumn>>, Pair<MatchableTableColumn, Pair<Integer, MatchableTableColumn>>> filterDataTypeMapper = new RecordMapper<Pair<Pair<MatchableTableColumn,Pair<Integer,Integer>>,Pair<Integer, MatchableTableColumn>>, Pair<MatchableTableColumn,Pair<Integer, MatchableTableColumn>>>() {

			private static final long serialVersionUID = 1L;

			@Override
			public void mapRecord(Pair<Pair<MatchableTableColumn, Pair<Integer, Integer>>, Pair<Integer, MatchableTableColumn>> record,
					DataIterator<Pair<MatchableTableColumn, Pair<Integer, MatchableTableColumn>>> resultCollector) {
				
				Pair<MatchableTableColumn, Pair<Integer, MatchableTableColumn>> webTableColumnToDBpediaColumn = new Pair<MatchableTableColumn, Pair<Integer, MatchableTableColumn>>(record.getFirst().getFirst(), record.getSecond());

				if(webTableColumnToDBpediaColumn.getFirst().getType()==webTableColumnToDBpediaColumn.getSecond().getSecond().getType()) {
					// only create a pair if the data types match!
					resultCollector.next(webTableColumnToDBpediaColumn);
				}
			}
		};
		
		// this transformation results in <web table column, dbpedia column>
		Processable<Pair<MatchableTableColumn, Pair<Integer, MatchableTableColumn>>> blockedColumns = columnsJoinedViaRefinedClasses.map(filterDataTypeMapper);
		
		// now we only need to get all instance correspondences that are relevant for a given column combination
		// so we group all pairs and instance correspondences by both table ids
		
		Function<String, Pair<MatchableTableColumn, Pair<Integer, MatchableTableColumn>>> pairToTableIds = new Function<String, Pair<MatchableTableColumn,Pair<Integer, MatchableTableColumn>>>() {
			
			private static final long serialVersionUID = 1L;

			@Override
			public String execute(Pair<MatchableTableColumn, Pair<Integer, MatchableTableColumn>> input) {
				return String.format("%d/%d", input.getFirst().getTableId(), input.getSecond().getFirst());
			}
		};
		
		Function<String, Correspondence<MatchableTableRow, Matchable>> instanceCorrespondenceToTableIds = new Function<String, Correspondence<MatchableTableRow, Matchable>>() {

			private static final long serialVersionUID = 1L;

			@Override
			public String execute(Correspondence<MatchableTableRow, Matchable> input) {
				return String.format("%d/%d", input.getFirstRecord().getTableId(), input.getSecondRecord().getTableId());
			}
		};
		
		// finally, we want to transform the result into objects of type BlockedMatchable
		RecordMapper<Pair<Iterable<Pair<MatchableTableColumn, Pair<Integer, MatchableTableColumn>>>, Iterable<Correspondence<MatchableTableRow, Matchable>>>, Correspondence<MatchableTableColumn, MatchableTableRow>> blockedMatchableMapper = new RecordMapper<Pair<Iterable<Pair<MatchableTableColumn,Pair<Integer, MatchableTableColumn>>>,Iterable<Correspondence<MatchableTableRow, Matchable>>>, Correspondence<MatchableTableColumn,MatchableTableRow>>() {
			
			private static final long serialVersionUID = 1L;

			@Override
			public void mapRecord(
					Pair<Iterable<Pair<MatchableTableColumn, Pair<Integer, MatchableTableColumn>>>, Iterable<Correspondence<MatchableTableRow, Matchable>>> record,
					DataIterator<Correspondence<MatchableTableColumn, MatchableTableRow>> resultCollector) {
				
				// record contains all data (column pairs and instance correspondences) for a combination of two tables
				// the BlockedMatchable that we want contains two columns and a list of instance correspondences
				
				// so, we first iterate over all combinations of columns
				if(record.getFirst()!=null) {
					for(Pair<MatchableTableColumn, Pair<Integer, MatchableTableColumn>> columnPair : record.getFirst()) {
						
						// and then create a BlockedMatchable with all instance correspondences
						Processable<Correspondence<MatchableTableRow, Matchable>> instanceCorrespondences = new ProcessableCollection<>();
						for(Correspondence<MatchableTableRow, Matchable> cor : record.getSecond()) {
							instanceCorrespondences.add(cor);
						}
						
						if (columnPair.getFirst().getType() == DataType.numeric){
							if(columnPair.getFirst().getStatistics().getKurtosis() >= 2){
								Correspondence<MatchableTableColumn, MatchableTableRow> blocked = 
										new Correspondence<MatchableTableColumn, MatchableTableRow>(
												columnPair.getFirst(), 
												columnPair.getSecond().getSecond(), 
												1.0,
												instanceCorrespondences);
							
								resultCollector.next(blocked);
							}else
								continue;
						}else{
							Correspondence<MatchableTableColumn, MatchableTableRow> blocked = 
									new Correspondence<MatchableTableColumn, MatchableTableRow>(
											columnPair.getFirst(), 
											columnPair.getSecond().getSecond(), 
											1.0,
											instanceCorrespondences);
						
							resultCollector.next(blocked);
						}
					}
				}
			}
		};
		
		// this coGroup results in BlockedMatchable
		Processable<Correspondence<MatchableTableColumn, MatchableTableRow>> blockedData = blockedColumns.coGroup(instanceCorrespondences, pairToTableIds, instanceCorrespondenceToTableIds, blockedMatchableMapper);
		
		calculatePerformance(schema1, schema2, blockedData);
		
		return blockedData;
	}


}
