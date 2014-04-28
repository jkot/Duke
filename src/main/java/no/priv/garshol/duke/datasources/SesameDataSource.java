package no.priv.garshol.duke.datasources;

import java.io.File;

import no.priv.garshol.duke.Record;
import no.priv.garshol.duke.RecordIterator;

import org.openrdf.model.Value;
import org.openrdf.query.BindingSet;
import org.openrdf.query.QueryEvaluationException;
import org.openrdf.query.QueryLanguage;
import org.openrdf.query.TupleQuery;
import org.openrdf.query.TupleQueryResult;
import org.openrdf.repository.Repository;
import org.openrdf.repository.RepositoryConnection;
import org.openrdf.repository.RepositoryException;
import org.openrdf.repository.sail.SailRepository;
import org.openrdf.sail.nativerdf.NativeStore;

public class SesameDataSource extends ColumnarDataSource {
  private String datafolder;
  private String query;

  public SesameDataSource() {
    super();
  }

  public void setDatafolder(String str) {
    this.datafolder = str;
  }

  public void setQuery(String query) {
    this.query = query;
  }

  public String getQuery() {
    return this.query;
  }
  
  public String getDatafolder() {
	  return this.datafolder;
  }

  
  public RecordIterator getRecords() {
    verifyProperty(datafolder, "datafolder");
    verifyProperty(query, "query");

	logger.info("Connecting to Sesame Native store in " + datafolder);
	Repository repo = new SailRepository(new NativeStore(new File(
			datafolder)));
	try {
		repo.initialize();
	} catch (RepositoryException e) {
		throw new RuntimeException(
				"Problem initializing Sesame native store: "
						+ e.getMessage(), e);
	}
	
	try {
		RepositoryConnection con = repo.getConnection();
		TupleQueryResult result = null;
			TupleQuery tupleQuery = con.prepareTupleQuery(QueryLanguage.SPARQL, query);

		  result = tupleQuery.evaluate();
		  
		  return new SesameIterator(con, result);
	} catch (Exception e) {
		throw new RuntimeException("Problem working with Sesame Native store: " + e.getMessage(), e);
	}    
  }

  protected String getSourceName() {
    return "Sesame";
  }
  
  public class SesameIterator extends RecordIterator {
    private TupleQueryResult rs;
    private boolean next;
    private RecordBuilder builder;
    private RepositoryConnection con;

    public SesameIterator(RepositoryConnection con, TupleQueryResult rs) throws QueryEvaluationException {
      this.rs = rs;
      this.next = rs.hasNext();
      this.builder = new RecordBuilder(SesameDataSource.this);
      this.con = con;
    }
    
    public boolean hasNext() {
      return next;
    }

    public Record next() {
      try {
		BindingSet bindingSet = rs.next();
		
		logger.info("BindingSet: " + bindingSet.toString());
    	  
        builder.newRecord();
        for (Column col : getColumns()) {
        	Value val = bindingSet.getValue(col.getName());
        	if (val != null) {
        		builder.addValue(col, val.stringValue());
        	}
        }

        next = rs.hasNext(); 

        return builder.getRecord();
      } catch (QueryEvaluationException e) {
    	  throw new RuntimeException(e);
	  }
    }

    public void close() {
        try {
            rs.close();
        	con.close();
        } catch (QueryEvaluationException e) {
        	throw new RuntimeException(e);
		} catch (RepositoryException e) {
        	throw new RuntimeException(e);
		}
    }
  }
}
