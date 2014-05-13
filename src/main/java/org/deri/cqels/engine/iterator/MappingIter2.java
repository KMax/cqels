package org.deri.cqels.engine.iterator;

import org.deri.cqels.engine.ExecContext;

public abstract class MappingIter2 extends MappingIter {
		
	private MappingIterator left,right;
	
	public MappingIter2(MappingIterator left, MappingIterator right, ExecContext context){
		super(context);
		this.left = left;
		this.right = right;
	}
	
	protected MappingIterator getLeft() { return left; };
	
	protected MappingIterator getRight(){ return right; };
	
	  @Override
	    protected final
	    void closeIterator() {
	        closeSubIterator();
	        performClose(left);
	        performClose(right);
	        left = null;
	        right = null;
	    }
	    
	    @Override
	    protected final
	    void requestCancel() {
	        performRequestCancel(left);
	        performRequestCancel(right);
	    }
	    
	    /** Cancellation of the query execution is happening */
	    protected abstract void requestSubCancel();
	    
	    /** Pass on the close method - no need to close the left or right QueryIterators passed to the QueryIter1 constructor */
	    protected abstract void closeSubIterator();
}
