package org.deri.cqels.engine.iterator;

import org.deri.cqels.engine.ExecContext;

public abstract class MappingIter1 extends MappingIter {
	
	private MappingIterator input;
	
	public MappingIter1(MappingIterator input, ExecContext context) {
		super(context);
		this.input = input;
		
	}
	
	protected MappingIterator getInput() {
		return input;
	};
	
	protected final void closeIterator() {
        closeSubIterator();
        performClose(input);
        input = null;
    }
    
    @Override
    protected final
    void requestCancel() {
        requestSubCancel();
        performRequestCancel(input);
    }
    
    /** Cancellation of the query execution is happening */
    protected abstract void requestSubCancel();
    
    /** Pass on the close method - no need to close the QueryIterator passed to the QueryIter1 constructor */
    protected abstract void closeSubIterator();

}
