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

import java.io.File;
import java.io.IOException;

import com.beust.jcommander.Parameter;

import de.uni_mannheim.informatik.dws.winter.matching.MatchingEvaluator;
import de.uni_mannheim.informatik.dws.winter.model.Correspondence;
import de.uni_mannheim.informatik.dws.winter.model.MatchingGoldStandard;
import de.uni_mannheim.informatik.dws.winter.model.Performance;
import de.uni_mannheim.informatik.dws.winter.model.Correspondence.RecordId;
import de.uni_mannheim.informatik.dws.winter.processing.Processable;
import de.uni_mannheim.informatik.dws.winter.utils.Executable;

/**
 * A command-line tool to evaluate correspondences that have been written to a file
 * 
 * @author Oliver Lehmberg (oli@dwslab.de)
 *
 */
public class EvaluateCorrespondences extends Executable {

	@Parameter(names = "-correspondences", required=true)
	private String correspondencesLocation;
	
	@Parameter(names = "-goldstandard", required=true)
	private String goldstandardLocation;
	
	@Parameter(names = "-canonicalization")
	private String canonicalizationLocation;
	
	@Parameter(names = "-gsComplete")
	private boolean gsIsComplete = false;
	
	@Parameter(names = "-verbose")
	private boolean isVerbose = false;
	
	public static void main(String[] args) throws IOException {
		EvaluateCorrespondences eval = new EvaluateCorrespondences();
		
		if(eval.parseCommandLine(EvaluateCorrespondences.class, args)) {
		
			eval.run();
			
		}
	}
	
	public void run() throws IOException {
		
		MatchingGoldStandard gs = new MatchingGoldStandard();
		gs.loadFromCSVFile(new File(goldstandardLocation));
		gs.setComplete(gsIsComplete);
		
		Processable<Correspondence<RecordId, RecordId>> cors = Correspondence.loadFromCsv(new File(correspondencesLocation));
		
		if(canonicalizationLocation!=null) {
			Canonicalizer canon = new Canonicalizer();
			canon.load(new File(canonicalizationLocation));
			
			for(Correspondence<RecordId, RecordId> cor : cors.get()) {
				cor.getSecondRecord().setIdentifier(canon.canonicalize(cor.getSecondRecord().getIdentifier()));
			}
		}
		
		System.out.println(String.format("%,d schema correspondences", cors.size()));
		
		MatchingEvaluator<RecordId, RecordId> eval = new MatchingEvaluator<>(isVerbose);
		Performance perf = eval.evaluateMatching(cors.get(), gs);
		
		System.out
		.println(String.format(
				"Performance:\n\tPrecision: %.4f\n\tRecall: %.4f\n\tF1: %.4f",
				perf.getPrecision(), perf.getRecall(),
				perf.getF1()));
	}
	
}
