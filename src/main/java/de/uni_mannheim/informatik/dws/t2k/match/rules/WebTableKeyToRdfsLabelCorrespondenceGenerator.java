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

import de.uni_mannheim.informatik.dws.t2k.match.data.MatchableTableColumn;
import de.uni_mannheim.informatik.dws.t2k.match.data.MatchableTableRow;
import de.uni_mannheim.informatik.dws.winter.model.Correspondence;
import de.uni_mannheim.informatik.dws.winter.model.Pair;
import de.uni_mannheim.informatik.dws.winter.processing.DatasetIterator;
import de.uni_mannheim.informatik.dws.winter.processing.RecordMapper;

/**
 * 
 * Generates correspondences between the detected entity label column and rdfs:label
 * 
 * @author Oliver Lehmberg (oli@dwslab.de)
 *
 */
public class WebTableKeyToRdfsLabelCorrespondenceGenerator implements RecordMapper<Pair<Integer, MatchableTableColumn>, Correspondence<MatchableTableColumn, MatchableTableRow>> {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private MatchableTableColumn rdfsLabel;
	
	
	public WebTableKeyToRdfsLabelCorrespondenceGenerator(MatchableTableColumn rdfsLabel) {
		this.rdfsLabel = rdfsLabel;
	}
	
	/* (non-Javadoc)
	 * @see de.uni_mannheim.informatik.wdi.processing.RecordMapper#mapRecord(java.lang.Object, de.uni_mannheim.informatik.wdi.processing.DatasetIterator)
	 */
	@Override
	public void mapRecord(Pair<Integer, MatchableTableColumn> record,
			DatasetIterator<Correspondence<MatchableTableColumn, MatchableTableRow>> resultCollector) {
		
		resultCollector.next(new Correspondence<MatchableTableColumn, MatchableTableRow>(record.getSecond(), rdfsLabel, 1.0, null));
		
	}
	

}
