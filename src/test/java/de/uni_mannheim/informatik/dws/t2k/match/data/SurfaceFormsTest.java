/**
 * 
 */
package de.uni_mannheim.informatik.dws.t2k.match.data;

import java.util.HashMap;

import de.uni_mannheim.informatik.dws.winter.utils.query.Q;
import junit.framework.TestCase;

/**
 * @author Sanikumar
 *
 */
public class SurfaceFormsTest extends TestCase {
	/**
	 * Test method for {@link de.uni_mannheim.informatik.dws.t2k.match.data.SurfaceForms#loadSurfaceForms()}
	 */
	
	public void testLoadSurfaceForms(){
		HashMap<String, String[]> surfaceForms = new HashMap<>();
		
		String[] sf1 = new String[] { "Deutschland", "Germany" };
		String[] sf2 = new String[] { "Hindustan", "Bharat" };
		
		surfaceForms.put("Germany",  sf1);
		surfaceForms.put("India", sf2);
		
		SurfaceForms sf = new SurfaceForms(surfaceForms);
		
//		checks whether we get correct surface forms for given entity?
		assertEquals(Q.toSet(sf1), sf.getSurfaceForms("Germany"));
		assertEquals(Q.toSet(sf2), sf.getSurfaceForms("India"));

//		check for null pointer exception if no surface forms were found for given entity
		assertNotNull(sf.getSurfaceForms("abc"));
	}
}
