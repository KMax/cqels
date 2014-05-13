package org.deri.cqels.engine.iterator;

import java.util.NoSuchElementException;

import org.deri.cqels.data.Mapping;

import org.openjena.atlas.logging.Log;

import com.hp.hpl.jena.query.QueryCancelledException;
import com.hp.hpl.jena.query.QueryException;
import com.hp.hpl.jena.query.QueryFatalException;
import com.hp.hpl.jena.sparql.util.Utils;

public abstract class MappingIteratorBase implements MappingIterator {
	
	private boolean finished = false;
	private boolean requestingCancel = false;
	private volatile boolean abortIterator = false ;
	
	protected abstract void closeIterator();
	protected abstract boolean hasNextMapping();
	protected abstract Mapping moveToNextMapping();
	protected abstract void requestCancel();
	    
	public boolean hasNext() {
		
		return hasNextMapping();
	}

	public Mapping next() {
		
		return nextMapping();
	}

	protected boolean isFinished() { 
		return finished; 
	}
    
	public final void remove() {
        Log.warn(this, "Call to QueryIterator.remove() : "+Utils.className(this)+".remove");
        throw new UnsupportedOperationException(Utils.className(this)+".remove");
    }

	public Mapping nextMapping() {
		try {
            if (abortIterator) {
                throw new QueryCancelledException();
            }
            if (finished) {
                // If abortIterator set after finished.
                if (abortIterator) {
                    throw new QueryCancelledException();
                }
                throw new NoSuchElementException(Utils.className(this));
            }
            
            if (!hasNextMapping()) {
                throw new NoSuchElementException(Utils.className(this));
            }
    
            Mapping obj = moveToNextMapping();
            if (obj == null) { 
                throw new NoSuchElementException(Utils.className(this));
			}
            if (requestingCancel && ! finished) {
                // But .cancel sets both requestingCancel and abortIterator
                // This only happens with a continuing iterator.
        		close();
        	}
            
            return obj;
        } catch (QueryFatalException ex) { 
            Log.fatal(this, "QueryFatalException", ex); 
            //abort ? 
            throw ex; 
        }
	}
	
	 protected static void performRequestCancel(MappingIterator iter) {
	        if ( iter == null ) {
	        	return;
	        }
	        iter.cancel();
	 }
	 
	 protected static void performClose(MappingIterator iter) {
	        if ( iter == null ) {
	        	return;
	        }
	        iter.close();
	 } 
	 
	  public void cancel() {
		  if (!this.requestingCancel) {
    	      synchronized (this) {
    	        this.requestCancel();
    	        this.requestingCancel = true;
    	        this.abortIterator = true;
              }
    	  }
	  }
	
	  public void close() {
		  if (finished) {
            return;
		  }
          try { 
        	  closeIterator(); 
          }
          catch (QueryException ex) { 
        	  Log.warn(this, "QueryException in close()", ex); 
          } 
          finished = true;
	  }

}
