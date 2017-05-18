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
package de.uni_mannheim.informatik.dws.t2k.similarity;

import de.uni_mannheim.informatik.dws.winter.similarity.string.TokenizingJaccardSimilarity;
import de.uni_mannheim.informatik.dws.winter.webtables.WebTablesStringNormalizer;

/**
 * The similarity function used in the original T2K matcher
 * @author Oliver Lehmberg (oli@dwslab.de)
 *
 */
public class WebJaccardStringSimilarity extends TokenizingJaccardSimilarity {

	private static final long serialVersionUID = 1L;
	
	/** (non-Javadoc)
	 * 
	 * Calculates similarity between the first and second string.
	 * 
	 * @see de.uni_mannheim.informatik.dws.winter.similarity.SimilarityMeasure#calculate(java.lang.Object, java.lang.Object)
	 * 
	 * @param first
	 * 			the first string (can be null)
	 * @param second
	 * 			the second string (can be null)
	 * @return the similarity score between the first and second string
	 */
	@Override
	public double calculate(String first, String second) {
        if(first==null || second==null) {
            return 0.0;
        }
        
        String s1 = WebTablesStringNormalizer.normaliseValue(first, true) + "";
        String s2 = WebTablesStringNormalizer.normaliseValue(second, true) + "";
        
        return super.calculate(s1, s2);
	}

}
