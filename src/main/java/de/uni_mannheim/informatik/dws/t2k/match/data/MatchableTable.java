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
package de.uni_mannheim.informatik.dws.t2k.match.data;

import de.uni_mannheim.informatik.dws.winter.model.Matchable;

/**
 * 
 * Model of a Web Table
 * 
 * @author Oliver Lehmberg (oli@dwslab.de)
 *
 */
public class MatchableTable implements Matchable {

	private int id;
	private String path;
	
	protected MatchableTable() {} // constructor for deserialisation
	
	public MatchableTable(int id, String path) {
		this.id = id;
		this.path = path;
	}
	
	/**
	 * @return the id
	 */
	public int getId() {
		return id;
	}
	
	/**
	 * @return the path
	 */
	public String getPath() {
		return path;
	}
	
	/* (non-Javadoc)
	 * @see de.uni_mannheim.informatik.wdi.model.Matchable#getIdentifier()
	 */
	@Override
	public String getIdentifier() {
		return path;
	}

	/* (non-Javadoc)
	 * @see de.uni_mannheim.informatik.wdi.model.Matchable#getProvenance()
	 */
	@Override
	public String getProvenance() {
		return null;
	}

}
