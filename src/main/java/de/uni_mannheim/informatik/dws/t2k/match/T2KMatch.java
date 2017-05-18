package de.uni_mannheim.informatik.dws.t2k.match;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Serializable;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import au.com.bytecode.opencsv.CSVWriter;

import com.beust.jcommander.Parameter;

import de.uni_mannheim.informatik.dws.t2k.match.comparators.MatchableTableRowComparator;
import de.uni_mannheim.informatik.dws.t2k.match.comparators.MatchableTableRowComparatorBasedOnSurfaceForms;
import de.uni_mannheim.informatik.dws.t2k.match.comparators.MatchableTableRowDateComparator;
import de.uni_mannheim.informatik.dws.t2k.match.components.CandidateFiltering;
import de.uni_mannheim.informatik.dws.t2k.match.components.CandidateRefinement;
import de.uni_mannheim.informatik.dws.t2k.match.components.CandidateSelection;
import de.uni_mannheim.informatik.dws.t2k.match.components.ClassDecision;
import de.uni_mannheim.informatik.dws.t2k.match.components.ClassRefinement;
import de.uni_mannheim.informatik.dws.t2k.match.components.CombineSchemaCorrespondences;
import de.uni_mannheim.informatik.dws.t2k.match.components.DuplicateBasedSchemaMatching;
import de.uni_mannheim.informatik.dws.t2k.match.components.IdentityResolution;
import de.uni_mannheim.informatik.dws.t2k.match.components.LabelBasedSchemaMatching;
import de.uni_mannheim.informatik.dws.t2k.match.components.TableFiltering;
import de.uni_mannheim.informatik.dws.t2k.match.components.UpdateSchemaCorrespondences;
import de.uni_mannheim.informatik.dws.t2k.match.data.ExtractedTriple;
import de.uni_mannheim.informatik.dws.t2k.match.data.KnowledgeBase;
import de.uni_mannheim.informatik.dws.t2k.match.data.MatchableTable;
import de.uni_mannheim.informatik.dws.t2k.match.data.MatchableTableColumn;
import de.uni_mannheim.informatik.dws.t2k.match.data.MatchableTableRow;
import de.uni_mannheim.informatik.dws.t2k.match.data.SurfaceForms;
import de.uni_mannheim.informatik.dws.t2k.match.data.WebTables;
import de.uni_mannheim.informatik.dws.t2k.match.rules.WebTableKeyToRdfsLabelCorrespondenceGenerator;
import de.uni_mannheim.informatik.dws.winter.index.IIndex;
import de.uni_mannheim.informatik.dws.winter.index.io.DefaultIndex;
import de.uni_mannheim.informatik.dws.winter.index.io.InMemoryIndex;
import de.uni_mannheim.informatik.dws.winter.matching.MatchingEngine;
import de.uni_mannheim.informatik.dws.winter.matching.MatchingEvaluator;
import de.uni_mannheim.informatik.dws.winter.model.Correspondence;
import de.uni_mannheim.informatik.dws.winter.model.MatchingGoldStandard;
import de.uni_mannheim.informatik.dws.winter.model.Pair;
import de.uni_mannheim.informatik.dws.winter.model.Performance;
import de.uni_mannheim.informatik.dws.winter.model.io.CSVCorrespondenceFormatter;
import de.uni_mannheim.informatik.dws.winter.preprocessing.datatypes.DataType;
import de.uni_mannheim.informatik.dws.winter.processing.DataAggregator;
import de.uni_mannheim.informatik.dws.winter.processing.DatasetIterator;
import de.uni_mannheim.informatik.dws.winter.processing.Processable;
import de.uni_mannheim.informatik.dws.winter.processing.ProcessableCollection;
import de.uni_mannheim.informatik.dws.winter.processing.RecordKeyValueMapper;
import de.uni_mannheim.informatik.dws.winter.processing.aggregators.CountAggregator;
import de.uni_mannheim.informatik.dws.winter.similarity.date.WeightedDateSimilarity;
import de.uni_mannheim.informatik.dws.winter.similarity.numeric.PercentageSimilarity;
import de.uni_mannheim.informatik.dws.winter.similarity.string.GeneralisedStringJaccard;
import de.uni_mannheim.informatik.dws.winter.similarity.string.LevenshteinSimilarity;
import de.uni_mannheim.informatik.dws.winter.utils.BuildInfo;
import de.uni_mannheim.informatik.dws.winter.utils.Executable;
import de.uni_mannheim.informatik.dws.winter.utils.MapUtils;
import de.uni_mannheim.informatik.dws.winter.utils.StringUtils;
import de.uni_mannheim.informatik.dws.winter.utils.parallel.Parallel;
import de.uni_mannheim.informatik.dws.winter.utils.query.Func;
import de.uni_mannheim.informatik.dws.winter.utils.query.Q;

/**
 * 
 * Executable class for the T2K Match algorithm.
 * 
 * See Ritze, D., Lehmberg, O., & Bizer, C. (2015, July). Matching html tables to dbpedia. In Proceedings of the 5th International Conference on Web Intelligence, Mining and Semantics (p. 10). ACM.
 * 
 * 
 * @author Oliver Lehmberg (oli@dwslab.de)
 *
 */
public class T2KMatch extends Executable implements Serializable {
	
	private static final long serialVersionUID = 1L;

	@Parameter(names = "-sf")
	private String sfLocation;
	
	@Parameter(names = "-kb", required=true)
	private String kbLocation;
	
	@Parameter(names = "-web", required=true)
	private String webLocation;
	
	@Parameter(names = "-identityGS")
	private String identityGSLocation;
	
	@Parameter(names = "-schemaGS")
	private String schemaGSLocation;
	
	@Parameter(names = "-classGS")
	private String classGSLocation;
	
	@Parameter(names = "-index")
	private String indexLocation;
	
	@Parameter(names = "-sparkMaster")
	private String sparkMaster;
	
	@Parameter(names = "-sparkJar")
	private String sparkJar;
	
	@Parameter(names = "-results", required=true)
	private String resultLocation;
	
	@Parameter(names = "-ontology", required=true)
	private String ontologyLocation;
	
	@Parameter(names = "-readGS")
	private String readGSLocation;
	
	@Parameter(names = "-writeGS")
	private String writeGSLocation;

	@Parameter(names = "-rd")
	private String rdLocation;

	@Parameter(names = "-verbose")
	private boolean verbose = false;
	
	@Parameter(names = "-detectKeys")
	private boolean detectKeys;
	
	/*******
	 * Parameters for algorithm configuration
	 *******/
	@Parameter(names = "-mappedRatio")
	private double par_mappedRatio=0.0;
	
	@Parameter(names = "-numIterations")
	private int numIterations = 1;
	
    public static void main( String[] args ) throws Exception
    {
    	T2KMatch t2k = new T2KMatch();
    	
    	if(t2k.parseCommandLine(T2KMatch.class, args)) {
    		
    		t2k.initialise();
    		
    		t2k.match();
    		
    	}
    }
    
    private IIndex index;
    private KnowledgeBase kb;
    private WebTables web;
    private MatchingGoldStandard instanceGs;
    private MatchingGoldStandard schemaGs;
    private MatchingGoldStandard classGs;
    private SurfaceForms sf;
    private File results;
    
    public void initialise() throws IOException {	 
    	if(sfLocation==null && rdLocation==null){
    		sf = new SurfaceForms(null, null);
    	}else if(sfLocation==null && rdLocation!=null){
    		sf = new SurfaceForms(null, new File(rdLocation));
    	}else if(sfLocation!=null && rdLocation==null){
    		sf = new SurfaceForms(new File(sfLocation), null);
    	}else{
    		sf = new SurfaceForms(new File(sfLocation), new File(rdLocation));
    	}
    	
    	boolean createIndex = false;
    	// create index for candidate lookup
    	if(indexLocation==null) {
    		// no index provided, create a new one in memory
    		index = new InMemoryIndex();
    		createIndex = true;
    	} else{
    		// load index from location that was provided
    		index = new DefaultIndex(indexLocation);
    		createIndex = !new File(indexLocation).exists();
    	}
    	if(createIndex) {
    		sf.loadIfRequired();
    	}
    	
    	//first load DBpedia class Hierarchy
    	KnowledgeBase.loadClassHierarchy(ontologyLocation);
    	
    	// load knowledge base, fill index if it is empty
		kb = KnowledgeBase.loadKnowledgeBase(new File(kbLocation), createIndex?index:null, sf);

    	// load instance gold standard
    	if(identityGSLocation!=null) {
	    	File instGsFile = new File(identityGSLocation);
	    	if(instGsFile.exists()) {
		    	instanceGs = new MatchingGoldStandard();
		    	instanceGs.loadFromCSVFile(instGsFile);
		    	instanceGs.setComplete(true);
	    	}
    	}
    	
    	// load schema gold standard
    	if(schemaGSLocation!=null) {
    		File schemaGsFile = new File(schemaGSLocation);
    		if(schemaGsFile.exists()) {
				schemaGs = new MatchingGoldStandard();
				schemaGs.loadFromCSVFile(schemaGsFile);
				schemaGs.setComplete(true);
    		}
    	}

    	// load class gold standard
    	if(classGSLocation!=null) {
    		File classGsFile = new File(classGSLocation);
    		if(classGsFile.exists()) {
				classGs = new MatchingGoldStandard();
				classGs.loadFromCSVFile(classGsFile);
				classGs.setComplete(true);
    		}
    	}
    	
    	if(sparkJar==null) {
    		sparkJar = BuildInfo.getJarPath(this.getClass()).getAbsolutePath();
    	}
    	
    	results = new File(resultLocation);
    	if(!results.exists()) {
    		results.mkdirs();
    	}
    }
    
    public void match() throws Exception {
    	/***********************************************
    	 * Matching Framework Initialisation
    	 ***********************************************/
    	// create matching engine
    	MatchingEngine<MatchableTableRow, MatchableTableColumn> matchingEngine = new MatchingEngine<>();
    	// disable stack-trace logging for long-running tasks
    	Parallel.setReportIfStuck(false);
    	
		web = WebTables.loadWebTables(new File(webLocation), false, true, detectKeys);

    	/***********************************************
    	 * Gold Standard Adjustment
    	 ***********************************************/
    	// remove all correspondences from the GS for tables that were not loaded
    	if(instanceGs!=null) {
    		instanceGs.removeNonexistingExamples(web.getRecords());
    	}
    	if(schemaGs!=null) {
    		schemaGs.removeNonexistingExamples(web.getSchema());
    	}
    	if(classGs!=null) {
    		classGs.removeNonexistingExamples(web.getTables());
    	}
    	
    	
    	/***********************************************
    	 * Key Preparation
    	 ***********************************************/
    	// create schema correspondences between the key columns and rdfs:Label
    	Processable<Correspondence<MatchableTableColumn, MatchableTableRow>> keyCorrespondences = web.getKeys().transform(new WebTableKeyToRdfsLabelCorrespondenceGenerator(kb.getRdfsLabel()));
    	if(verbose) {
    		for(Correspondence<MatchableTableColumn, MatchableTableRow> cor : keyCorrespondences.get()) {
    			System.out.println(String.format("%s: [%d]%s", web.getTableNames().get(cor.getFirstRecord().getTableId()), cor.getFirstRecord().getColumnIndex(), cor.getFirstRecord().getHeader()));
    		}
    	}
    	
    	/***********************************************
    	 * Candidate Selection
    	 ***********************************************/
    	MatchingLogger.printHeader("Candidate Selection");
    	CandidateSelection cs = new CandidateSelection(matchingEngine, sparkMaster!=null, index, indexLocation, web, kb, sf, keyCorrespondences);
    	Processable<Correspondence<MatchableTableRow, MatchableTableColumn>> instanceCorrespondences = cs.run();
    	evaluateInstanceCorrespondences(instanceCorrespondences, "candidate");
    	if(verbose) {
    		printCandidateStatistics(instanceCorrespondences);
    	}
    	
    	/***********************************************
    	 *Candidate Selection - Class Decision
    	 ***********************************************/
    	MatchingLogger.printHeader("Candidate Selection - Class Decision");
    	ClassDecision classDec = new ClassDecision();
    	Map<Integer, Set<String>> classesPerTable = classDec.runClassDecision(kb, instanceCorrespondences, matchingEngine);
    	evaluateClassCorrespondences(createClassCorrespondences(classesPerTable), "instance-based");
    	
    	/***********************************************
    	 *Candidate Selection - Candidate Refinement
    	 ***********************************************/
    	MatchingLogger.printHeader("Candidate Selection - Candidate Refinement");
    	CandidateRefinement cr = new CandidateRefinement(matchingEngine, sparkMaster!=null, index, indexLocation, web, kb, sf, keyCorrespondences, classesPerTable);
    	instanceCorrespondences = cr.run();
    	evaluateInstanceCorrespondences(instanceCorrespondences, "refined candidate");
    	if(verbose) {
    		printCandidateStatistics(instanceCorrespondences);
    	}
   
    	/***********************************************
    	 *Candidate Selection - Property-based Class Refinement
    	 ***********************************************/
    	MatchingLogger.printHeader("Property-based Class Refinement");
    	// match properties
    	DuplicateBasedSchemaMatching schemaMatchingForClassRefinement = new DuplicateBasedSchemaMatching(matchingEngine, web, kb, sf, classesPerTable, instanceCorrespondences, false);
    	schemaMatchingForClassRefinement.setFinalPropertySimilarityThreshold(0.03);
    	Processable<Correspondence<MatchableTableColumn, MatchableTableRow>> schemaCorrespondences = schemaMatchingForClassRefinement.run();
    	// add key correspondences (some tables only have key correspondences)
    	evaluateSchemaCorrespondences(schemaCorrespondences, "duplicate-based (refinement)");
    	schemaCorrespondences = schemaCorrespondences.append(keyCorrespondences);
    	// determine most probable class mapping
    	ClassRefinement classRefinement = new ClassRefinement(kb.getPropertyIndices(), KnowledgeBase.getClassHierarchy(),schemaCorrespondences,classesPerTable, kb.getClassIds());
    	classesPerTable = classRefinement.run();
    	Map<Integer, String> finalClassPerTable = classRefinement.getFinalClassPerTable();
    	evaluateClassCorrespondences(createClassCorrespondence(finalClassPerTable), "schema-based");
    	
    	/***********************************************
    	 *Candidate Selection - Class-based Filtering
    	 ***********************************************/
    	CandidateFiltering classFilter = new CandidateFiltering(classesPerTable, kb.getClassIndices(), instanceCorrespondences);
    	instanceCorrespondences = classFilter.run();
    	evaluateInstanceCorrespondences(instanceCorrespondences, "property refined candidate");
    	if(verbose) {
    		printCandidateStatistics(instanceCorrespondences);
    	}
    	
    	/***********************************************
    	 *Iterative Matching
    	 ***********************************************/
    	Processable<Correspondence<MatchableTableColumn, MatchableTableRow>> labelBasedSchemaCorrespondences = null;
    	Processable<Correspondence<MatchableTableColumn, MatchableTableRow>> lastSchemaCorrespondences = null;
    	
    	LabelBasedSchemaMatching labelBasedSchema = new LabelBasedSchemaMatching(matchingEngine, web, kb, classesPerTable, instanceCorrespondences);
    	DuplicateBasedSchemaMatching duplicateBasedSchema = new DuplicateBasedSchemaMatching(matchingEngine, web, kb, sf, classesPerTable, instanceCorrespondences, false);
    	CombineSchemaCorrespondences combineSchema = new CombineSchemaCorrespondences(keyCorrespondences);
    	combineSchema.setVerbose(verbose);
    	IdentityResolution identityResolution = new IdentityResolution(matchingEngine, web, kb, sf);
    	UpdateSchemaCorrespondences updateSchema = new UpdateSchemaCorrespondences();
    	
    	int iteration = 0;
    	do { // iterative matching loop
    		/***********************************************
	    	 * Schema Matching - Label Based
	    	 ***********************************************/
    		MatchingLogger.printHeader("Schema Matching - Label Based");
    		labelBasedSchema.setInstanceCorrespondences(instanceCorrespondences);
    		labelBasedSchemaCorrespondences = labelBasedSchema.run();
    		evaluateSchemaCorrespondences(labelBasedSchemaCorrespondences, "label-based");
    		
	    	/***********************************************
	    	 * Schema Matching - Duplicate Based
	    	 ***********************************************/
	    	MatchingLogger.printHeader("Schema Matching - Duplicate Based");
	    	duplicateBasedSchema.setInstanceCorrespondences(instanceCorrespondences);
	    	schemaCorrespondences = duplicateBasedSchema.run();
	    	evaluateSchemaCorrespondences(schemaCorrespondences, "duplicate-based");
	    	
	    	/***********************************************
	    	 * Combine Schema Correspondences
	    	 ***********************************************/
	    	MatchingLogger.printHeader("Combine Schema Correspondences");
	    	combineSchema.setSchemaCorrespondences(schemaCorrespondences);
	    	combineSchema.setLabelBasedSchemaCorrespondences(labelBasedSchemaCorrespondences);
	    	schemaCorrespondences = combineSchema.run();
	    	evaluateSchemaCorrespondences(schemaCorrespondences, "combined");

	    	/***********************************************
	    	 * Iterative - Update Schema Correspondences
	    	 ***********************************************/
	    	if(lastSchemaCorrespondences!=null) {
	    		updateSchema.setSchemaCorrespondences(lastSchemaCorrespondences);
	    		updateSchema.setNewSchemaCorrespondences(schemaCorrespondences);
	    		schemaCorrespondences = updateSchema.run();
	    		evaluateSchemaCorrespondences(schemaCorrespondences, "updated");
	    	}
	    	
	    	/***********************************************
	    	 * Identity Resolution
	    	 ***********************************************/
	    	MatchingLogger.printHeader("Identity Resolution");
	    	identityResolution.setInstanceCorrespondences(instanceCorrespondences);
	    	identityResolution.setSchemaCorrespondences(schemaCorrespondences);
	    	instanceCorrespondences = identityResolution.run();
	    	evaluateInstanceCorrespondences(instanceCorrespondences, "final");
	    	if(verbose) {
	    		printCandidateStatistics(instanceCorrespondences);
	    	}

    	
	    	lastSchemaCorrespondences = schemaCorrespondences;
    	} while(++iteration<numIterations); // loop for iterative part
    	
    	/***********************************************
    	 * One-to-one Matching
    	 ***********************************************/
    	instanceCorrespondences = matchingEngine.getTopKInstanceCorrespondences(instanceCorrespondences, 1, 0.0);
    	schemaCorrespondences = matchingEngine.getTopKSchemaCorrespondences(schemaCorrespondences, 1, 0.0);

    	/***********************************************
    	 *Table Filtering - Mapped Ratio Filter
    	 ***********************************************/
    	if(par_mappedRatio>0.0) {
	    	TableFiltering tableFilter = new TableFiltering(web, instanceCorrespondences, classesPerTable, schemaCorrespondences);
	    	tableFilter.setMinMappedRatio(par_mappedRatio);
	    	tableFilter.run();
	    	classesPerTable = tableFilter.getClassesPerTable();
	    	instanceCorrespondences = tableFilter.getInstanceCorrespondences();
	    	schemaCorrespondences = tableFilter.getSchemaCorrespondences();
    	}
    	
    	/***********************************************
    	 * Evaluation
    	 ***********************************************/
    	evaluateInstanceCorrespondences(instanceCorrespondences, "");
    	evaluateSchemaCorrespondences(schemaCorrespondences, "");
		evaluateClassCorrespondences(createClassCorrespondence(finalClassPerTable), "");
		
    	/***********************************************
    	 * Write Results
    	 ***********************************************/
		new CSVCorrespondenceFormatter().writeCSV(new File(results, "instance_correspondences.csv"), instanceCorrespondences);
		new CSVCorrespondenceFormatter().writeCSV(new File(results, "schema_correspondences.csv"), schemaCorrespondences);
		
    	HashMap<Integer, String> inverseTableIndices = (HashMap<Integer, String>) MapUtils.invert(web.getTableIndices());
		CSVWriter csvWriter = new CSVWriter(new FileWriter(new File(results, "class_decision.csv")));
		for(Integer tableId : classesPerTable.keySet()) {
			csvWriter.writeNext(new String[] {tableId.toString(), inverseTableIndices.get(tableId), classesPerTable.get(tableId).toString().replaceAll("\\[", "").replaceAll("\\]", "")});
		}
		csvWriter.close();

		// generate triples from the matched tables and evaluate using Local-Closed-World Assumption (Note: no fusion happened so far, so values won't be too good...)
		TripleGenerator tripleGen = new TripleGenerator(web, kb);
		tripleGen.setComparatorForType(DataType.string, new MatchableTableRowComparatorBasedOnSurfaceForms(new GeneralisedStringJaccard(new LevenshteinSimilarity(), 0.5, 0.5), kb.getPropertyIndices(), 0.5, sf, true));
		tripleGen.setComparatorForType(DataType.numeric, new MatchableTableRowComparator<>(new PercentageSimilarity(0.05), kb.getPropertyIndices(), 0.00));
		tripleGen.setComparatorForType(DataType.date, new MatchableTableRowDateComparator(new WeightedDateSimilarity(1, 3, 5), kb.getPropertyIndices(), 0.9));
		Processable<ExtractedTriple> triples = tripleGen.run(instanceCorrespondences, schemaCorrespondences);
		System.out.println(String.format("Extracted %d existing (%.4f%% match values in KB) and %d new triples!", tripleGen.getExistingTripleCount(), tripleGen.getCorrectTripleCount()*100.0/(double)tripleGen.getExistingTripleCount(), tripleGen.getNewTripleCount()));
		ExtractedTriple.writeCSV(new File(results, "extracted_triples.csv"), triples.get());
		
		//TODO add the correspondences to the tables and write them to the disk
    }
	
    protected void evaluateInstanceCorrespondences(Processable<Correspondence<MatchableTableRow, MatchableTableColumn>> instanceCorrespondences, String name) {
    	Performance instancePerf = null;
    	if(instanceGs!=null) {
    		instanceCorrespondences.deduplicate();
	    	MatchingEvaluator<MatchableTableRow, MatchableTableColumn> instanceEvaluator = new MatchingEvaluator<>(false);
	    	Collection<Correspondence<MatchableTableRow, MatchableTableColumn>> instanceCorrespondencesCollection = instanceCorrespondences.get();
	    	System.out.println(String.format("%d %s instance correspondences", instanceCorrespondencesCollection.size(), name));
	    	instancePerf = instanceEvaluator.evaluateMatching(instanceCorrespondencesCollection, instanceGs);
    	}

		if(instancePerf!=null) {
			System.out
			.println(String.format(
					"Instance Performance:\n\tPrecision: %.4f\n\tRecall: %.4f\n\tF1: %.4f",
					instancePerf.getPrecision(), instancePerf.getRecall(),
					instancePerf.getF1()));
		}
    }
    
    protected void evaluateSchemaCorrespondences(Processable<Correspondence<MatchableTableColumn, MatchableTableRow>> schemaCorrespondences, String name) {
    	Performance schemaPerf = null;
		if(schemaGs!=null) {
			schemaCorrespondences.deduplicate();
			MatchingEvaluator<MatchableTableColumn, MatchableTableRow> schemaEvaluator = new MatchingEvaluator<>(false);
			Collection<Correspondence<MatchableTableColumn, MatchableTableRow>> schemaCorrespondencesCollection = schemaCorrespondences.get();
			System.out.println(String.format("%d %s schema correspondences", schemaCorrespondencesCollection.size(), name));
			schemaPerf = schemaEvaluator.evaluateMatching(schemaCorrespondencesCollection, schemaGs);
		}
		
		if(schemaPerf!=null) {
			System.out
			.println(String.format(
					"Schema Performance:\n\tPrecision: %.4f\n\tRecall: %.4f\n\tF1: %.4f",
					schemaPerf.getPrecision(), schemaPerf.getRecall(),
					schemaPerf.getF1()));
		}	
    }
    
    protected void evaluateClassCorrespondences(Processable<Correspondence<MatchableTable, MatchableTableColumn>> classCorrespondences, String name) {
    	Performance classPerf = null;
		if(classGs!=null) {
			classCorrespondences.deduplicate();
			MatchingEvaluator<MatchableTable, MatchableTableColumn> classEvaluator = new MatchingEvaluator<>(false);
			Collection<Correspondence<MatchableTable, MatchableTableColumn>> classCorrespondencesCollection = classCorrespondences.get();
			System.out.println(String.format("%d %s class correspondences", classCorrespondencesCollection.size(), name));
			classPerf = classEvaluator.evaluateMatching(classCorrespondencesCollection, classGs);
		}
		
		if(classPerf!=null) {
			System.out
			.println(String.format(
					"Class Performance:\n\tPrecision: %.4f\n\tRecall: %.4f\n\tF1: %.4f",
					classPerf.getPrecision(), classPerf.getRecall(),
					classPerf.getF1()));
		}
    }
    
    protected Processable<Correspondence<MatchableTable, MatchableTableColumn>> createClassCorrespondences(Map<Integer, Set<String>> classesPerTable) {
    	//TODO the class matching should be replaced by actual matchers that create correspondences, such that we don't need this method
    	Processable<Correspondence<MatchableTable, MatchableTableColumn>> result = new ProcessableCollection<>();
    	
    	for(int tableId : classesPerTable.keySet()) {
    		
    		MatchableTable webTable = web.getTables().getRecord(web.getTableNames().get(tableId));
    		
    		for(String className : classesPerTable.get(tableId)) {
    			
    			MatchableTable kbTable = kb.getTables().getRecord(className);

    			Correspondence<MatchableTable, MatchableTableColumn> cor = new Correspondence<MatchableTable, MatchableTableColumn>(webTable, kbTable, 1.0, null);
    			result.add(cor);
    		}
    		
    	}
    	
    	return result;
    }
    protected Processable<Correspondence<MatchableTable, MatchableTableColumn>> createClassCorrespondence(Map<Integer, String> classPerTable) {
    	//TODO the class matching should be replaced by actual matchers that create correspondences, such that we don't need this method
    	Processable<Correspondence<MatchableTable, MatchableTableColumn>> result = new ProcessableCollection<>();
    	
    	for(int tableId : classPerTable.keySet()) {
    		
    		MatchableTable webTable = web.getTables().getRecord(web.getTableNames().get(tableId));
    		
    		String className = classPerTable.get(tableId);
    			
			MatchableTable kbTable = kb.getTables().getRecord(className);

			Correspondence<MatchableTable, MatchableTableColumn> cor = new Correspondence<MatchableTable, MatchableTableColumn>(webTable, kbTable, 1.0, null);
			result.add(cor);
    		
    	}
    	
    	return result;
    }
    
    protected void printCandidateStatistics(Processable<Correspondence<MatchableTableRow, MatchableTableColumn>> instanceCorrespondences) {
    	
    	RecordKeyValueMapper<String, Correspondence<MatchableTableRow, MatchableTableColumn>, Correspondence<MatchableTableRow, MatchableTableColumn>> groupBy = new RecordKeyValueMapper<String, Correspondence<MatchableTableRow,MatchableTableColumn>, Correspondence<MatchableTableRow,MatchableTableColumn>>() {

			private static final long serialVersionUID = 1L;

			@Override
			public void mapRecordToKey(Correspondence<MatchableTableRow, MatchableTableColumn> record,
					DatasetIterator<Pair<String, Correspondence<MatchableTableRow, MatchableTableColumn>>> resultCollector) {
				
				String tableName = web.getTableNames().get(record.getFirstRecord().getTableId());
				
				resultCollector.next(new Pair<String, Correspondence<MatchableTableRow,MatchableTableColumn>>(tableName, record));
				
			}
		};
		Processable<Pair<String, Integer>> counts = instanceCorrespondences.aggregateRecords(groupBy, new CountAggregator<String, Correspondence<MatchableTableRow, MatchableTableColumn>>());
		
		// get class distribution
		DataAggregator<String, Correspondence<MatchableTableRow, MatchableTableColumn>, Map<String, Integer>> classAggregator = new DataAggregator<String, Correspondence<MatchableTableRow,MatchableTableColumn>, Map<String,Integer>>() {
			
			private static final long serialVersionUID = 1L;

			@Override
			public Map<String, Integer> initialise(String keyValue) {
				return new HashMap<>();
			}
			
			@Override
			public Map<String, Integer> aggregate(Map<String, Integer> previousResult,
					Correspondence<MatchableTableRow, MatchableTableColumn> record) {
				
				String className = kb.getClassIndices().get(record.getSecondRecord().getTableId()); 
				
				Integer cnt = previousResult.get(className);
				if(cnt==null) {
					cnt = 0;
				}
				
				previousResult.put(className, cnt+1);
				
				return previousResult;
			}
		};
		
		Processable<Pair<String, Map<String, Integer>>> classDistribution = instanceCorrespondences.aggregateRecords(groupBy, classAggregator);
		final Map<String, Map<String, Integer>> classesByTable = Pair.toMap(classDistribution.get());
		
		System.out.println("Candidates per Table:");
		for(final Pair<String, Integer> p : counts.get()) {
			System.out.println(String.format("\t%s\t%d", p.getFirst(), p.getSecond()));
			
			Collection<Pair<String, Integer>> classCounts = Q.sort(Pair.fromMap(classesByTable.get(p.getFirst())), new Comparator<Pair<String, Integer>>() {

				@Override
				public int compare(Pair<String, Integer> o1, Pair<String, Integer> o2) {
					return -Integer.compare(o1.getSecond(), o2.getSecond());
				}
			});
			
			System.out.println(String.format("\t\t%s", StringUtils.join(Q.project(classCounts, new Func<String, Pair<String, Integer>>() {

				@Override
				public String invoke(Pair<String, Integer> in) {
					return String.format("%s: %.4f%%", in.getFirst(), in.getSecond()*100.0/(double)p.getSecond());
				}
				
			}), ", ")));
		}
    	
    }
}
