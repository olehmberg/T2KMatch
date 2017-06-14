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

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * 
 * Canonocaliser used for converting equivalent URIs.
 * 
 * If multiple URIs, for example for DBpedia properties, are considered as equivalent, this class converts them into a canonical form.
 * 
 * For example, both http://dbpedia.org/ontology/areaTotal and http://dbpedia.org/ontology/PopulatedPlace/areaTotal refer to the same property (semantically).
 * 
 * @author Oliver Lehmberg (oli@dwslab.de)
 *
 */
public class Canonicalizer {

	private Map<String, String> canonicalForms = new HashMap<>();
	
	public void load(File location) throws IOException {
		
		BufferedReader r = new BufferedReader(new FileReader(location));
		
		String line = null;
		
		while((line = r.readLine())!=null) {
			
			String[] values = line.split("\t");
			
			if(values.length>1) {
				String canonical = values[0];
				
				for(int i = 1; i<values.length; i++) {
					canonicalForms.put(values[i], canonical);
				}
			}
			
		}
		
		r.close();
	}
	
	public String canonicalize(String value) {
		if(canonicalForms.containsKey(value)) {
			return canonicalForms.get(value);
		} else {
			return value;
		}
	}
}
