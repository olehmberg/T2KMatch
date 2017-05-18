package de.uni_mannheim.informatik.dws.t2k.match.comparators;

import de.uni_mannheim.informatik.dws.t2k.match.data.MatchableTableColumn;
import de.uni_mannheim.informatik.dws.t2k.match.data.MatchableTableRow;
import de.uni_mannheim.informatik.dws.winter.matching.rules.Comparator;
import de.uni_mannheim.informatik.dws.winter.model.SimpleCorrespondence;
import de.uni_mannheim.informatik.dws.winter.similarity.SimilarityMeasure;

/**
 * 
 * Comparator for label-based schema matching.
 * 
 * @author Oliver Lehmberg (oli@dwslab.de)
 *
 */
public class SchemaLabelComparator implements Comparator<MatchableTableColumn, MatchableTableRow>{

	private static final long serialVersionUID = 1L;
	private SimilarityMeasure<String> similarity = null;
	private double sim;
	
	public SchemaLabelComparator(SimilarityMeasure<String> measure) {
		super();
		this.similarity = measure;
	}

	/*
	 * (non-Javadoc)
	 * @see de.uni_mannheim.informatik.dws.winter.matching.rules.Comparator#compare(de.uni_mannheim.informatik.dws.winter.model.Matchable, de.uni_mannheim.informatik.dws.winter.model.Matchable, de.uni_mannheim.informatik.dws.winter.model.SimpleCorrespondence)
	 */
	@Override
	public double compare(
			MatchableTableColumn record1,
			MatchableTableColumn record2,
			SimpleCorrespondence<MatchableTableRow> schemaCorrespondences) {
		sim = similarity.calculate(record1.getHeader(), record2.getHeader());

		return sim;
	}

}
