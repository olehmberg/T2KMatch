package de.uni_mannheim.informatik.dws.t2k.similarity;

import junit.framework.TestCase;

/**
 * @author Sanikumar
 *
 */
public class WebJaccardStringSimilarityTest extends TestCase {
	/**
	 * Test method for {@link de.uni_mannheim.informatik.dws.t2k.similarity.WebJaccardStringSimilarity#calculate(String, String)}
	 */

	public void testCalculate(){
		WebJaccardStringSimilarity sm = new WebJaccardStringSimilarity();
		
//		calculate similarity
		assertEquals(1.0, sm.calculate("republic", "republic"));
		assertEquals(0.5, sm.calculate("republic", "republic party"));
		assertEquals(0.0, sm.calculate("republic", "democratic"));
	}
}
