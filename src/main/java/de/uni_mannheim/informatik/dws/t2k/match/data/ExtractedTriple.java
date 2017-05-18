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

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Serializable;
import java.util.Collection;

import au.com.bytecode.opencsv.CSVWriter;
import de.uni_mannheim.informatik.dws.winter.preprocessing.datatypes.DataType;

/**
 * 
 * Model of a triple that was extracted from a Web Table.
 * 
 * @author Oliver Lehmberg (oli@dwslab.de)
 *
 */
public class ExtractedTriple implements Serializable {

	private static final long serialVersionUID = 1L;
	private String subjectURI;
	private String subjectValue;
	private double subjectConfidence;
	
	private String predicateURI;
	private String predicateValue;
	private double predicateConfidence;
	
	private String objectValue;
	private String objectValueInKB;
	private boolean objectValueMatchesKB;
	private double evaluatedSimilarity;
	
	private DataType dataType;
	
	private String sourceURL;
	private String sourceTable;
	private int sourceColumnIndex;
	
	public String getSubjectURI() {
		return subjectURI;
	}
	public void setSubjectURI(String subjectURI) {
		this.subjectURI = subjectURI;
	}
	public String getSubjectValue() {
		return subjectValue;
	}
	public void setSubjectValue(String subjectValue) {
		this.subjectValue = subjectValue;
	}
	public double getSubjectConfidence() {
		return subjectConfidence;
	}
	public void setSubjectConfidence(double subjectConfidence) {
		this.subjectConfidence = subjectConfidence;
	}
	public String getPredicateURI() {
		return predicateURI;
	}
	public void setPredicateURI(String predicateURI) {
		this.predicateURI = predicateURI;
	}
	public String getPredicateValue() {
		return predicateValue;
	}
	public void setPredicateValue(String predicateValue) {
		this.predicateValue = predicateValue;
	}
	public double getPredicateConfidence() {
		return predicateConfidence;
	}
	public void setPredicateConfidence(double predicateConfidence) {
		this.predicateConfidence = predicateConfidence;
	}
	public String getObjectValue() {
		return objectValue;
	}
	public void setObjectValue(String objectValue) {
		this.objectValue = objectValue;
	}
	public String getObjectValueInKB() {
		return objectValueInKB;
	}
	public void setObjectValueInKB(String objectValueInKB) {
		this.objectValueInKB = objectValueInKB;
	}
	public String getSourceURL() {
		return sourceURL;
	}
	public void setSourceURL(String sourceURL) {
		this.sourceURL = sourceURL;
	}
	public String getSourceTable() {
		return sourceTable;
	}
	public void setSourceTable(String sourceTable) {
		this.sourceTable = sourceTable;
	}
	public int getSourceColumnIndex() {
		return sourceColumnIndex;
	}
	public void setSourceColumnIndex(int sourceColumnIndex) {
		this.sourceColumnIndex = sourceColumnIndex;
	}
	public boolean isObjectValueMatchesKB() {
		return objectValueMatchesKB;
	}
	public void setObjectValueMatchesKB(boolean objectValueMatchesKB) {
		this.objectValueMatchesKB = objectValueMatchesKB;
	}
	public DataType getDataType() {
		return dataType;
	}
	public void setDataType(DataType dataType) {
		this.dataType = dataType;
	}
	public double getEvaluatedSimilarity() {
		return evaluatedSimilarity;
	}
	public void setEvaluatedSimilarity(double evaluatedSimilarity) {
		this.evaluatedSimilarity = evaluatedSimilarity;
	}
	public ExtractedTriple() {}
	
	public ExtractedTriple(String subjectURI, String subjectValue, double subjectConfidence, String predicateURI,
			String predicateValue, double predicateConfidence, String objectValue, String objectValueInKB,
			String sourceURL, String sourceTable, int sourceColumnIndex) {
		super();
		this.subjectURI = subjectURI;
		this.subjectValue = subjectValue;
		this.subjectConfidence = subjectConfidence;
		this.predicateURI = predicateURI;
		this.predicateValue = predicateValue;
		this.predicateConfidence = predicateConfidence;
		this.objectValue = objectValue;
		this.objectValueInKB = objectValueInKB;
		this.sourceURL = sourceURL;
		this.sourceTable = sourceTable;
		this.sourceColumnIndex = sourceColumnIndex;
	}

	public static void writeCSV(File f, Collection<ExtractedTriple> triples) throws IOException {
		CSVWriter w = new CSVWriter(new FileWriter(f));
		
		w.writeNext(new String[] {
				"URL",
				"Table",
				"Column Index",
				"Subject URI",
				"Subject Value",
				"Subject Confidence",
				"Predicate URI",
				"Predicate Value",
				"Predicate Confidence",
				"Data Type",
				"Object Value",
				"Object Value in KB",
				"Object Value matches KB",
				"Object Value Similarity"
		});
		
		for(ExtractedTriple t : triples) {
			w.writeNext(new String[] {
					t.getSourceURL(),
					t.getSourceTable(),
					Integer.toString(t.getSourceColumnIndex()),
					t.getSubjectURI(),
					t.getSubjectValue(),
					Double.toString(t.getSubjectConfidence()),
					t.getPredicateURI(),
					t.getPredicateValue(),
					Double.toString(t.getPredicateConfidence()),
					t.getDataType().toString(),
					t.getObjectValue(),
					t.getObjectValueInKB(),
					Boolean.toString(t.isObjectValueMatchesKB()),
					Double.toString(t.getEvaluatedSimilarity())
			});
		}
		
		w.close();
	}
	
}
