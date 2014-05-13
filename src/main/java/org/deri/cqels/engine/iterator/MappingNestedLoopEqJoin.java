package org.deri.cqels.engine.iterator;

import org.deri.cqels.data.Mapping;
import org.deri.cqels.engine.ExecContext;
import org.deri.cqels.engine.OpRouter;
import com.hp.hpl.jena.sparql.ARQInternalErrorException;

public class MappingNestedLoopEqJoin extends MappingIter {
	private MappingIterator curItr,leftItr;
	private Mapping nextMapping;
	OpRouter leftRouter, rightRouter;
	public MappingNestedLoopEqJoin(ExecContext context, OpRouter leftRouter, 
				OpRouter rightRouter) {
		super(context);
		this.leftRouter = leftRouter;
		leftItr = leftRouter.getBuff();
		this.rightRouter = rightRouter;
		//System.out.println("MappingNestedLoopEqJoin");
	}

	@Override
	protected void closeIterator() {
		leftItr.close();
		if(curItr != null) {
			curItr.close();
		}
	}

	@Override
	protected boolean hasNextMapping() {
		if(isFinished()) {
			return false;
		}
		if(nextMapping != null) {
			return true;
		}
		nextMapping = move2Next();
		return nextMapping != null;
	}
	
	//Move to next Mapping
	private Mapping move2Next() {
		//System.out.println("move cursor "+Thread.currentThread());
		while(true) {
			if(curItr != null) {
				if(curItr.hasNext()) {
					return curItr.nextMapping();
				}
				curItr.close();
				curItr = null;
			}
			curItr = eqJoinWorker();
			if(curItr == null) {
				return null;
			}
			//System.out.println("loop move2Next");
		}
	}
	
	private MappingIterator eqJoinWorker() {
		if(leftItr == null || !leftItr.hasNext()) {
			return null;//NullMappingIter.instance();
		}
		//System.out.println("call for another filter ");
		return rightRouter.searchBuff4Match(leftItr.next());
	}
	
	@Override
	protected Mapping moveToNextMapping() {
		if ( nextMapping == null ) {
            throw new ARQInternalErrorException("moveToNextMapping: slot empty but hasNext was true)") ;
		}
        Mapping m = nextMapping;
        nextMapping = move2Next();
        return m;
	}

	@Override
	protected void requestCancel() {
		performRequestCancel(leftItr);
		if(curItr != null) {
			performRequestCancel(curItr);
		}
	}


}
