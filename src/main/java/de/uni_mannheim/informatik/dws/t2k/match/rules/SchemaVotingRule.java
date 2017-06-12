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
package de.uni_mannheim.informatik.dws.t2k.match.rules;

import java.util.HashMap;

import de.uni_mannheim.informatik.dws.t2k.match.comparators.MatchableTableRowComparator;
import de.uni_mannheim.informatik.dws.t2k.match.data.MatchableTableColumn;
import de.uni_mannheim.informatik.dws.t2k.match.data.MatchableTableRow;
import de.uni_mannheim.informatik.dws.winter.matching.rules.VotingMatchingRule;
import de.uni_mannheim.informatik.dws.winter.model.Correspondence;
import de.uni_mannheim.informatik.dws.winter.model.Matchable;
import de.uni_mannheim.informatik.dws.winter.preprocessing.datatypes.DataType;

/**
 * @author Oliver Lehmberg (oli@dwslab.de)
 *
 */
public class SchemaVotingRule extends VotingMatchingRule<MatchableTableColumn, MatchableTableRow> {

	/**
	 * @param finalThreshold
	 */
	public SchemaVotingRule(double finalThreshold) {
		super(finalThreshold);
		comparators = new HashMap<>();
	}

	private static final long serialVersionUID = 1L;

	private HashMap<DataType, MatchableTableRowComparator<?>> comparators;
	private int rdfsLabelId = -1;
	
	/**
	 * @param rdfsLabelId the rdfsLabelId to set
	 */
	public void setRdfsLabelId(int rdfsLabelId) {
		this.rdfsLabelId = rdfsLabelId;
	}
	
	public HashMap<DataType, MatchableTableRowComparator<?>> getComparators() {
		return comparators;
	}

	public int getRdfsLabelId() {
		return rdfsLabelId;
	}
	
	public void setComparatorForType(DataType type, MatchableTableRowComparator<?> comparator) {
		comparators.put(type, comparator);
	}
	
	/* (non-Javadoc)
	 * @see de.uni_mannheim.informatik.wdi.matching.VotingMatchingRule#compare(java.lang.Object, java.lang.Object, de.uni_mannheim.informatik.wdi.model.SimpleCorrespondence)
	 */
	@Override
	public double compare(MatchableTableColumn schemaElement1, MatchableTableColumn schemaElement2,
			Correspondence<MatchableTableRow, Matchable> correspondence) {
		
		// get the comparator for this data type
		MatchableTableRowComparator<?> cmp = comparators.get(schemaElement1.getType());
		
		if(rdfsLabelId==-1 || schemaElement2.getColumnIndex()!=rdfsLabelId) { // do not match the keys here
			if(cmp!=null) {
				// make sure the property exists in the second record and both columns have the same data type
				if(cmp.canCompareRecords(correspondence.getFirstRecord(), correspondence.getSecondRecord(), schemaElement1, schemaElement2)) {
					
					// calculate the similarity value
					return cmp.compare(correspondence.getFirstRecord(), correspondence.getSecondRecord(), schemaElement1, schemaElement2);
				}
			} else {
				System.out.println(String.format("[MISSING] no comparator for [%d]%s (%s)", schemaElement1.getColumnIndex(), schemaElement1.getHeader(), schemaElement1.getType(), schemaElement2.getHeader(), schemaElement2.getType()));
			}
		}
		
		return 0.0;
	}

}
