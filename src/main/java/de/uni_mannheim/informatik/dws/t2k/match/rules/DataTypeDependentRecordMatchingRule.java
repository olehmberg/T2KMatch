package de.uni_mannheim.informatik.dws.t2k.match.rules;

import java.util.HashMap;

import de.uni_mannheim.informatik.dws.t2k.match.comparators.MatchableTableRowComparator;
import de.uni_mannheim.informatik.dws.t2k.match.data.MatchableTableColumn;
import de.uni_mannheim.informatik.dws.t2k.match.data.MatchableTableRow;
import de.uni_mannheim.informatik.dws.winter.matching.rules.Comparator;
import de.uni_mannheim.informatik.dws.winter.matching.rules.FilteringMatchingRule;
import de.uni_mannheim.informatik.dws.winter.model.Correspondence;
import de.uni_mannheim.informatik.dws.winter.model.Matchable;
import de.uni_mannheim.informatik.dws.winter.preprocessing.datatypes.DataType;
import de.uni_mannheim.informatik.dws.winter.processing.Processable;

/**
 * 
 * A record matching rule that uses different comparators for each data type. The key value also has its own comparator and a weight that can be specified.
 * The similarity value is the sum of the similarities of the values weighted by the similaritiy of the schema correspondence between the values.
 * 
 * @author Oliver Lehmberg (oli@dwslab.de)
 *
 */
public class DataTypeDependentRecordMatchingRule extends FilteringMatchingRule<MatchableTableRow, MatchableTableColumn> {

	private static final long serialVersionUID = -2211353700799708359L;
	private HashMap<DataType, MatchableTableRowComparator<?>> comparators;
	private Comparator<MatchableTableRow, MatchableTableColumn> keyValueComparator;
	private double keyValueWeight = 1.0/3.0;
	private int rdfsLabelId;
	
	public void setComparatorForType(DataType type, MatchableTableRowComparator<?> comparator) {
		comparators.put(type, comparator);
	}
	
	public HashMap<DataType, MatchableTableRowComparator<?>> getComparators() {
		return comparators;
	}


	public Comparator<MatchableTableRow, MatchableTableColumn> getKeyValueComparator() {
		return keyValueComparator;
	}


	public double getKeyValueWeight() {
		return keyValueWeight;
	}


	public int getRdfsLabelId() {
		return rdfsLabelId;
	}


	/**
	 * @param keyValueComparator the keyValueComparator to set
	 */
	public void setKeyValueComparator(
			Comparator<MatchableTableRow, MatchableTableColumn> keyValueComparator) {
		this.keyValueComparator = keyValueComparator;
	}
	
	/**
	 * @param keyValueWeight the keyValueWeight to set
	 */
	public void setKeyValueWeight(double keyValueWeight) {
		this.keyValueWeight = keyValueWeight;
	}
	
	public DataTypeDependentRecordMatchingRule(double finalThreshold, int rdfsLabelId) {
		super(finalThreshold);
		comparators = new HashMap<>();
		this.rdfsLabelId = rdfsLabelId;
	}

	/*
	 * (non-Javadoc)
	 * @see de.uni_mannheim.informatik.dws.winter.matching.rules.FilteringMatchingRule#apply(de.uni_mannheim.informatik.dws.winter.model.Matchable, de.uni_mannheim.informatik.dws.winter.model.Matchable, de.uni_mannheim.informatik.dws.winter.processing.Processable)
	 */
	@Override
	public Correspondence<MatchableTableRow, MatchableTableColumn> apply(
			MatchableTableRow record1,
			MatchableTableRow record2,
			Processable<Correspondence<MatchableTableColumn, Matchable>> schemaCorrespondences) {
		double sum = 0.0;
		double sumOfWeights = 0.0;

		// sum up the similarity scores for all properties
		for(Correspondence<MatchableTableColumn, Matchable> sc : schemaCorrespondences.get()) {
			
			double sim = compare(record1, record2, sc);
			
			if(sim >= 0.0) {
				double weight;
				if(sc.getSecondRecord().getColumnIndex()!=rdfsLabelId) {
					weight = sc.getSimilarityScore();
				} else {
					weight = keyValueWeight;
				}
				
				sum += sim * weight;
				sumOfWeights += weight;
			}
			
//			if(sc.getSecondRecord().getColumnIndex()!=rdfsLabelId) {
//				MatchableTableRowComparator<?> cmp = comparators.get(record1.getType(sc.getFirstRecord().getColumnIndex()));
//				if(cmp!=null) {
//					// check if sc.getSecondRecord() exists as a property in record2
//					if(cmp.canCompareRecords(record1, record2, sc.getFirstRecord(), sc.getSecondRecord())) {
//						Double sim = cmp.compare(record1, record2, sc.getFirstRecord(), sc.getSecondRecord());
//						
//						double weight = sc.getSimilarityScore();
//						
//						sum += sim * weight;
//						sumOfWeights += sc.getSimilarityScore();
//					}
//				}
//			} else {
//				// the entity label value has a special weight and comparator
//				double keyValueSim = keyValueComparator.compare(record1, record2, sc);			
//				double otherValueSim = comparators.get(DataType.string).compare(record1, record2, sc.getFirstRecord(), sc.getSecondRecord());
//				
//				// combine similarity values
//				keyValueSim = (1.0/3.0) * keyValueSim + (2.0/3.0) * otherValueSim;
//				
//				sum += keyValueSim * keyValueWeight;
//				sumOfWeights += keyValueWeight;
//			}
		}
		
//		calculate final similarity. 
//		if sum of the similarity for all properties is '0' then set the similarity to '0', otherwise set the similarity to (sum/sumOfWeights)
		double sim = sum==0.0 ? 0.0 : (sum/sumOfWeights);
		
//		if the similarity satisfies threshold then return the correspondence between two records, 'null' otherwise
		if(sim>=getFinalThreshold()) {
			return new Correspondence<MatchableTableRow, MatchableTableColumn>(record1, record2, sim, schemaCorrespondences);
		} else {
			return null;
		}
	}

	/* (non-Javadoc)
	 * @see de.uni_mannheim.informatik.wdi.matching.Comparator#compare(de.uni_mannheim.informatik.wdi.model.Matchable, de.uni_mannheim.informatik.wdi.model.Matchable, de.uni_mannheim.informatik.wdi.model.SimpleCorrespondence)
	 */
	@Override
	public double compare(MatchableTableRow record1, MatchableTableRow record2,
			Correspondence<MatchableTableColumn, Matchable> schemaCorrespondence) {
		if(schemaCorrespondence.getSecondRecord().getColumnIndex()!=rdfsLabelId) {
			MatchableTableRowComparator<?> cmp = comparators.get(record1.getType(schemaCorrespondence.getFirstRecord().getColumnIndex()));
			if(cmp!=null) {
				// check if sc.getSecondRecord() exists as a property in record2
				if(cmp.canCompareRecords(record1, record2, schemaCorrespondence.getFirstRecord(), schemaCorrespondence.getSecondRecord())) {
					return cmp.compare(record1, record2, schemaCorrespondence.getFirstRecord(), schemaCorrespondence.getSecondRecord());
				}
			}
		} else {
			// the entity label value has a special weight and comparator
			double keyValueSim = keyValueComparator.compare(record1, record2, schemaCorrespondence);			
			double otherValueSim = comparators.get(DataType.string).compare(record1, record2, schemaCorrespondence.getFirstRecord(), schemaCorrespondence.getSecondRecord());
			
			// combine similarity values
			return (1.0/3.0) * keyValueSim + (2.0/3.0) * otherValueSim;
		}
		
		return -1;
	}


}
