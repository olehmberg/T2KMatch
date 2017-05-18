package de.uni_mannheim.informatik.dws.t2k.match.comparators;

import java.util.LinkedList;
import java.util.List;

import de.uni_mannheim.informatik.dws.t2k.match.data.MatchableTableColumn;
import de.uni_mannheim.informatik.dws.t2k.match.data.MatchableTableRow;
import de.uni_mannheim.informatik.dws.t2k.match.data.SurfaceForms;
import de.uni_mannheim.informatik.dws.winter.matching.rules.Comparator;
import de.uni_mannheim.informatik.dws.winter.model.SimpleCorrespondence;
import de.uni_mannheim.informatik.dws.winter.similarity.SimilarityMeasure;

/**
 * 
 * Comparator for entity label values.
 * 
 * @author Sanikumar
 *
 */
public class KeyValueComparatorBasedOnSurfaceForms implements Comparator<MatchableTableRow, MatchableTableColumn>{

	private static final long serialVersionUID = 1L;
	
//	similarity measure
	private SimilarityMeasure<String> measure = null;

	public SimilarityMeasure<String> getMeasure() {
		return measure;
	}
	
//	rdfs-label column index
	private int rdfsLabelId = -1;
	
//	surface form
	private SurfaceForms sf;
	
	public SurfaceForms getSf() {
		return sf;
	}

	/**
	 * @param rdfsLabelId the rdfsLabelId to set
	 */
	public void setRdfsLabelId(int rdfsLabelId) {
		this.rdfsLabelId = rdfsLabelId;
	}
	
	public KeyValueComparatorBasedOnSurfaceForms() {
	}

	public KeyValueComparatorBasedOnSurfaceForms(SimilarityMeasure<String> measure, int rdfsLabelId, SurfaceForms sf) {
		this.measure = measure;
		this.rdfsLabelId = rdfsLabelId;
		this.sf = sf;
	}
	
	/*
	 * (non-Javadoc)
	 * @see de.uni_mannheim.informatik.dws.winter.matching.rules.Comparator#compare(de.uni_mannheim.informatik.dws.winter.model.Matchable, de.uni_mannheim.informatik.dws.winter.model.Matchable, de.uni_mannheim.informatik.dws.winter.model.SimpleCorrespondence)
	 */
	@Override
	public double compare(
			MatchableTableRow record1,
			MatchableTableRow record2,
			SimpleCorrespondence<MatchableTableColumn> schemaCorrespondences) {
		
		if(schemaCorrespondences==null) {
			return 0.0;
		}
		
//		load surface-forms/redirects
		sf.loadIfRequired();
		
//		value to be compared from web table row
		String webTableKey = (String)record1.get(schemaCorrespondences.getFirstRecord().getColumnIndex());
//		value to be compared from dbpedia table row
		String label = (String)record2.get(rdfsLabelId);
		
//		 list all possible value alternatives for the web table value (+redirect) and the dbpedia value (+surface forms)
		List<String> values2 = new LinkedList<>();
		
		values2.add(label);
		for(String sForm : sf.getSurfaceForms(SurfaceForms.getNormalizedLabel(record2))){
			values2.add(sForm);
		}
		
		double sim = Double.MIN_VALUE;
		
//		 use the max. similarity between any two values
		for(String v2 : values2) {
			double s = measure.calculate(webTableKey, v2);
			
			sim = Math.max(s, sim);
		}

		return sim;
	}

}
