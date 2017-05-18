package de.uni_mannheim.informatik.dws.t2k.match;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringUtils;

import de.uni_mannheim.informatik.dws.t2k.match.data.MatchableTableColumn;
import de.uni_mannheim.informatik.dws.t2k.match.data.MatchableTableRow;
import de.uni_mannheim.informatik.dws.winter.model.Correspondence;
import de.uni_mannheim.informatik.dws.winter.processing.Processable;
import de.uni_mannheim.informatik.dws.winter.utils.Distribution;
import de.uni_mannheim.informatik.dws.winter.utils.query.Func;
import de.uni_mannheim.informatik.dws.winter.utils.query.Q;

/**
 * Utility class that provides methods for printing matching results.
 * 
 * @author Oliver Lehmberg (oli@dwslab.de)
 *
 */
public class MatchingLogger {

	public static void printHeader(String title) {
		System.out
				.println("**********************************************************************************");
		System.out.println(title);
		System.out
				.println("**********************************************************************************");
	}

	public static void logCandidateSelectionResult(
			Processable<Correspondence<MatchableTableRow, MatchableTableColumn>> correspondences) {
		// show candidates
		System.out.println("Candidates");
		Map<MatchableTableRow, Collection<Correspondence<MatchableTableRow, MatchableTableColumn>>> candidates = Q
				.group(correspondences.get(),
						new Func<MatchableTableRow, Correspondence<MatchableTableRow, MatchableTableColumn>>() {

							@Override
							public MatchableTableRow invoke(
									Correspondence<MatchableTableRow, MatchableTableColumn> in) {
								return in.getFirstRecord();
							}
						});

		for (MatchableTableRow instance : candidates.keySet()) {
			// System.out.println(instance.format(15));
			System.out.println(StringUtils.join(instance.getValues(), "|"));
			for (Correspondence<MatchableTableRow, MatchableTableColumn> correspondence : candidates
					.get(instance)) {
				System.out.println(String.format("\t%.2f\t[%s]", correspondence
						.getSimilarityScore(), correspondence.getSecondRecord()
						.get(1)));
			}
		}

		// show class distribution
		Distribution<Integer> classDist = ClassDistribution
				.getClassDistribution(correspondences.get());

		System.out.println("Class distribution");
		System.out.println(classDist.format());
	}

	public static void logSchemaMatchingResult(
			Processable<Correspondence<MatchableTableColumn, MatchableTableRow>> schemaCorrespondences,
			HashMap<String, String> webTableHeaders, List<String> dbp) {
		System.out.println("Schema Mapping");
		for (Correspondence<MatchableTableColumn, MatchableTableRow> c : schemaCorrespondences.get()) {
			System.out.println(String.format(
					"(%.2f) %s %s -> %s ",
					c.getSimilarityScore(),
					c.getFirstRecord().getIdentifier(),
					webTableHeaders.get(c.getFirstRecord().getIdentifier()),
					dbp.get(c.getSecondRecord().getColumnIndex())));
		}
	}

	public static void logIdentityResolutionResult(
			Processable<Correspondence<MatchableTableRow, MatchableTableColumn>> correspondences,
			boolean showFullMatch) {
		System.out.println("Instance Correspondences");
		Map<MatchableTableRow, Collection<Correspondence<MatchableTableRow, MatchableTableColumn>>> candidates = Q
				.group(correspondences.get(),
						new Func<MatchableTableRow, Correspondence<MatchableTableRow, MatchableTableColumn>>() {

							@Override
							public MatchableTableRow invoke(
									Correspondence<MatchableTableRow, MatchableTableColumn> in) {
								return in.getFirstRecord();
							}
						});

		for (MatchableTableRow instance : candidates.keySet()) {
			System.out.println(StringUtils.join(instance.getValues(), "|"));
			for (Correspondence<MatchableTableRow, MatchableTableColumn> correspondence : candidates
					.get(instance)) {
				
				if(showFullMatch) {
					System.out.println(String.format("\t%s", StringUtils.join(correspondence.getSecondRecord().getValues(), "|")));
				} else {
					System.out.println(String.format("\t%.2f\t[%s]", correspondence
							.getSimilarityScore(), correspondence.getSecondRecord()
							.get(1)));	
				}
			}
		}
	}
}
