package de.uni_mannheim.informatik.dws.t2k.index.dbpedia;

import java.io.IOException;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;

import org.apache.lucene.document.Document;
import org.apache.lucene.index.Term;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.queryparser.classic.QueryParserBase;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.BooleanClause.Occur;

import de.uni_mannheim.informatik.dws.winter.index.IIndex;
import de.uni_mannheim.informatik.dws.winter.index.io.StringTokeniser;
import de.uni_mannheim.informatik.dws.winter.index.management.IndexManagerBase;
import de.uni_mannheim.informatik.dws.winter.webtables.WebTablesStringNormalizer;

/**
 * 
 * An index for DBpedia instances.
 * 
 * @author Oliver Lehmberg (oli@dwslab.de)
 *
 */
public class DBPediaInstanceIndex extends IndexManagerBase{

	private static final long serialVersionUID = 1L;
	
	public DBPediaInstanceIndex() {
		
	}
	private boolean verbose = false;
    public boolean isVerbose() {
        return verbose;
    }
    public void setVerbose(boolean verbose) {
        this.verbose = verbose;
    }
    
	public DBPediaInstanceIndex(IIndex index, String defaultField) {
		super(index, defaultField);
	}

	private boolean removeBrackets = false;
	public boolean isRemoveBrackets() {
        return removeBrackets;
    }
	public void setRemoveBrackets(boolean removeBrackets) {
        this.removeBrackets = removeBrackets;
    }
	
   public List<DBpediaIndexEntry> searchMany(Collection<String> labels) {
        long start, setup=0, search=0, result=0;
        start = System.currentTimeMillis();
        List<DBpediaIndexEntry> results = new LinkedList<DBpediaIndexEntry>();

        IndexSearcher indexSearcher = getIndex().getIndexSearcher();

        QueryParser queryParser = getQueryParserFromCache();
        
        try {           
            Query q = null;
            
            q = new BooleanQuery();
            
            for(String lbl : labels) {
                Query q0 = null;
                String value = QueryParserBase.escape(lbl);
	            if(!isSearchExactMatches()) {
	                List<String> tokens = StringTokeniser.tokenise(value, isRemoveBrackets());
	                
	                StringBuilder sb = new StringBuilder();
	                
	                for(String token : tokens) {
	                    sb.append(token);
	                    
	                    if(getMaxEditDistance()>0) {
	                        sb.append("~");
	                        sb.append(getMaxEditDistance());
	                    }
	                    sb.append(" ");
	                }
	                
	                value = sb.toString();
	                
	                if(value.trim().length()>0) {
	                    q0 = queryParser.parse(value);
	                }
	            } else {
	                if(value.trim().length()>0) {
	                    q0 = new TermQuery(new Term(getDefaultField(), value));
	                }
	            }
	            
	            if(q0!=null) {
	                ((BooleanQuery)q).add(q0, Occur.SHOULD);
	            }
            }
            
            if(q!=null) {
                if(getFilterValues()!=null && getFilterValues().size()>0) {
                    BooleanQuery filter = new BooleanQuery();
                    
                    for(String s : getFilterValues()) {
                        filter.add(new TermQuery(new Term(getFilterField(), s)), Occur.SHOULD);
                    }
                    
                    BooleanQuery all = new BooleanQuery();
                    all.add(q, Occur.MUST);
                    all.add(filter, Occur.MUST);
                    
                    q = all;
                }
                
                if(isVerbose()) {
                    System.out.println("Query: \n" + labels.toString() + "\n" + q.toString());
                }
                
                setup = System.currentTimeMillis() - start;
                start = System.currentTimeMillis();
                
                int numResults = getNumRetrievedDocsFromIndex();
                ScoreDoc[] hits = indexSearcher.search(q, numResults).scoreDocs;
                
                search = System.currentTimeMillis() - start;
                start = System.currentTimeMillis();
                
                if(hits != null)
                {
                    if(isVerbose()) {
                        System.out.println(" found " + hits.length + " hits");
                    }
                    for (int i = 0; i < hits.length; i++) {
                        
                        Document doc = indexSearcher.doc(hits[i].doc);
        
                        DBpediaIndexEntry e = DBpediaIndexEntry.fromDocument(doc);

                        if(isVerbose()) {
                            System.out.println(e.getClass_label() + ": " + e.getLabel());
                        }
                        
                        results.add(e);
                    }
                }
            }
            
            result = System.currentTimeMillis() - start;

        } catch (IOException e) {
            e.printStackTrace();
        } catch (ParseException e1) {
            System.err.println(String.format("Parse exception for label '%s'", labels));
            e1.printStackTrace();
        }
        if(isVerbose()) {
            System.out.println(" returning " + results.size() + " documents");
            System.out.println(String.format("setup: %d\tsearch: %d\tload: %d", setup, search, result));
        }
        return results;
    }
	
	public List<DBpediaIndexEntry> search(String label) {
	    long start, setup=0, search=0, result=0;
	    start = System.currentTimeMillis();
		List<DBpediaIndexEntry> results = new LinkedList<DBpediaIndexEntry>();

		IndexSearcher indexSearcher = getIndex().getIndexSearcher();

		QueryParser queryParser = getQueryParserFromCache();
		
		try {			
			String value = QueryParserBase.escape(label); 
			Query q = null;
			
			if(!isSearchExactMatches()) {
    			
			    value = WebTablesStringNormalizer.normaliseValue(value, true);
			    
			    List<String> tokens = WebTablesStringNormalizer.tokenise(value, true);
			    
			    StringBuilder sb = new StringBuilder();
			    
			    for(String token : tokens) {
			        sb.append(token);
			        
			        if(getMaxEditDistance()>0) {
			            sb.append("~");
			            sb.append(getMaxEditDistance());
			        }
			        sb.append(" ");
			    }
			    
			    value = sb.toString();
    			
    			if(value.trim().length()>0) {
    			    q = queryParser.parse(value);
    			}
			} else {
			    if(value.trim().length()>0) {
			        q = new TermQuery(new Term(getDefaultField(), value));
			    }
			}
			
			if(q!=null) {
    			if(getFilterValues()!=null && getFilterValues().size()>0) {
    			    BooleanQuery filter = new BooleanQuery();
    			    
    			    for(String s : getFilterValues()) {
    			        filter.add(new TermQuery(new Term(getFilterField(), s)), Occur.SHOULD);
    			    }
    			    
    			    BooleanQuery all = new BooleanQuery();
    			    all.add(q, Occur.MUST);
    			    all.add(filter, Occur.MUST);
    			    
    			    q = all;
    			}
    			
    			if(isVerbose()) {
    			    System.out.println("Query: \n" + value + "\n" + q.toString());
    			}
    			
    			setup = System.currentTimeMillis() - start;
    			start = System.currentTimeMillis();
    			
    			int numResults = getNumRetrievedDocsFromIndex();
    			ScoreDoc[] hits = indexSearcher.search(q, numResults).scoreDocs;
    			
    			search = System.currentTimeMillis() - start;
    			start = System.currentTimeMillis();
    			
    			if(hits != null)
    			{
    			    if(isVerbose()) {
    			        System.out.println(" found " + hits.length + " hits");
    			    }
    				for (int i = 0; i < hits.length; i++) {
    					
    					Document doc = indexSearcher.doc(hits[i].doc);
    	
    					DBpediaIndexEntry e = DBpediaIndexEntry.fromDocument(doc);

    					if(isVerbose()) {
    					    System.out.println(e.getClass_label() + ": " + e.getLabel());
    					}
    					
    					results.add(e);
    				}
    			}
			} else {
			    if(isVerbose()) {
			        System.out.println(String.format("Empty query for '%s'", label));
			    }
			}
			
			result = System.currentTimeMillis() - start;

		} catch (IOException e) {
			e.printStackTrace();
		} catch (ParseException e1) {
		    System.err.println(String.format("Parse exception for label '%s'", label));
			e1.printStackTrace();
		}
		if(isVerbose()) {
		    System.out.println(" returning " + results.size() + " documents");
		    System.out.println(String.format("setup: %d\tsearch: %d\tload: %d", setup, search, result));
		}
		return results;
	}

	public List<DBpediaIndexEntry> search(String label, String filterField, Collection<String> filterValues) {
	    long start, setup=0, search=0, result=0;
	    start = System.currentTimeMillis();
		List<DBpediaIndexEntry> results = new LinkedList<DBpediaIndexEntry>();

		IndexSearcher indexSearcher = getIndex().getIndexSearcher();

		QueryParser queryParser = getQueryParserFromCache();
		
		try {			
			String value = QueryParserBase.escape(label); 
			Query q = null;
			
			if(!isSearchExactMatches()) {
    			
			    value = WebTablesStringNormalizer.normaliseValue(value, true);
			    
			    List<String> tokens = WebTablesStringNormalizer.tokenise(value, true);
			    
			    StringBuilder sb = new StringBuilder();
			    
			    for(String token : tokens) {
			        sb.append(token);
			        
			        if(getMaxEditDistance()>0) {
			            sb.append("~");
			            sb.append(getMaxEditDistance());
			        }
			        sb.append(" ");
			    }
			    
			    value = sb.toString();
    			
    			if(value.trim().length()>0) {
    			    q = queryParser.parse(value);
    			}
			} else {
			    if(value.trim().length()>0) {
			        q = new TermQuery(new Term(getDefaultField(), value));
			    }
			}
			
			if(q!=null) {
    			if(filterValues!=null && filterValues.size()>0) {
    			    BooleanQuery filter = new BooleanQuery();
    			    
    			    for(String s : filterValues) {
    			        filter.add(new TermQuery(new Term(filterField, s)), Occur.SHOULD);
    			    }
    			    
    			    BooleanQuery all = new BooleanQuery();
    			    all.add(q, Occur.MUST);
    			    all.add(filter, Occur.MUST);
    			    
    			    q = all;
    			}
    			
    			if(isVerbose()) {
    			    System.out.println("Query: \n" + value + "\n" + q.toString());
    			}
    			
    			setup = System.currentTimeMillis() - start;
    			start = System.currentTimeMillis();
    			
    			int numResults = getNumRetrievedDocsFromIndex();
    			ScoreDoc[] hits = indexSearcher.search(q, numResults).scoreDocs;
    			
    			search = System.currentTimeMillis() - start;
    			start = System.currentTimeMillis();
    			
    			if(hits != null)
    			{
    			    if(isVerbose()) {
    			        System.out.println(" found " + hits.length + " hits");
    			    }
    				for (int i = 0; i < hits.length; i++) {
    					
    					Document doc = indexSearcher.doc(hits[i].doc);
    	
    					DBpediaIndexEntry e = DBpediaIndexEntry.fromDocument(doc);

    					if(isVerbose()) {
    					    System.out.println(e.getClass_label() + ": " + e.getLabel());
    					}
    					
    					results.add(e);
    				}
    			}
			} else {
			    if(isVerbose()) {
			        System.out.println(String.format("Empty query for '%s'", label));
			    }
			}
			
			result = System.currentTimeMillis() - start;

		} catch (IOException e) {
			e.printStackTrace();
		} catch (ParseException e1) {
		    System.err.println(String.format("Parse exception for label '%s'", label));
			e1.printStackTrace();
		}
		if(isVerbose()) {
		    System.out.println(" returning " + results.size() + " documents");
		    System.out.println(String.format("setup: %d\tsearch: %d\tload: %d", setup, search, result));
		}
		return results;
	}
	
	public List<DBpediaIndexEntry> searchMany(Collection<String> labels, String filterField, Collection<String> filterValues){
		long start, setup=0, search=0, result=0;
        start = System.currentTimeMillis();
        List<DBpediaIndexEntry> results = new LinkedList<DBpediaIndexEntry>();

        IndexSearcher indexSearcher = getIndex().getIndexSearcher();

        QueryParser queryParser = getQueryParserFromCache();
        
        try {           
            Query q = null;
            
            q = new BooleanQuery();
            
            for(String lbl : labels) {
                Query q0 = null;
                String value = QueryParserBase.escape(lbl);
	            if(!isSearchExactMatches()) {
	                List<String> tokens = StringTokeniser.tokenise(value, isRemoveBrackets());
	                
	                StringBuilder sb = new StringBuilder();
	                
	                for(String token : tokens) {
	                    sb.append(token);
	                    
	                    if(getMaxEditDistance()>0) {
	                        sb.append("~");
	                        sb.append(getMaxEditDistance());
	                    }
	                    sb.append(" ");
	                }
	                
	                value = sb.toString();
	                
	                if(value.trim().length()>0) {
	                    q0 = queryParser.parse(value);
	                }
	            } else {
	                if(value.trim().length()>0) {
	                    q0 = new TermQuery(new Term(getDefaultField(), value));
	                }
	            }
	            
	            if(q0!=null) {
	                ((BooleanQuery)q).add(q0, Occur.SHOULD);
	            }
            }
            
            if(q!=null) {
                if(filterValues!=null && filterValues.size()>0) {
                    BooleanQuery filter = new BooleanQuery();
                    
                    for(String s : filterValues) {
                        filter.add(new TermQuery(new Term(filterField, s)), Occur.SHOULD);
                    }
                    
                    BooleanQuery all = new BooleanQuery();
                    all.add(q, Occur.MUST);
                    all.add(filter, Occur.MUST);
                    
                    q = all;
                }
                
                if(isVerbose()) {
                    System.out.println("Query: \n" + labels.toString() + "\n" + q.toString());
                }
                
                setup = System.currentTimeMillis() - start;
                start = System.currentTimeMillis();
                
                int numResults = getNumRetrievedDocsFromIndex();
                ScoreDoc[] hits = indexSearcher.search(q, numResults).scoreDocs;
                
                search = System.currentTimeMillis() - start;
                start = System.currentTimeMillis();
                
                if(hits != null)
                {
                    if(isVerbose()) {
                        System.out.println(" found " + hits.length + " hits");
                    }
                    for (int i = 0; i < hits.length; i++) {
                        
                        Document doc = indexSearcher.doc(hits[i].doc);
        
                        DBpediaIndexEntry e = DBpediaIndexEntry.fromDocument(doc);

                        if(isVerbose()) {
                            System.out.println(e.getClass_label() + ": " + e.getLabel());
                        }
                        
                        results.add(e);
                    }
                }
            }
            
            result = System.currentTimeMillis() - start;

        } catch (IOException e) {
            e.printStackTrace();
        } catch (ParseException e1) {
            System.err.println(String.format("Parse exception for label '%s'", labels));
            e1.printStackTrace();
        }
        if(isVerbose()) {
            System.out.println(" returning " + results.size() + " documents");
            System.out.println(String.format("setup: %d\tsearch: %d\tload: %d", setup, search, result));
        }
        return results;
	}
}
