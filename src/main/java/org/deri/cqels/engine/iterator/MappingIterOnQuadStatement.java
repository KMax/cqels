package org.deri.cqels.engine.iterator;

import org.deri.cqels.data.EnQuad;
import org.deri.cqels.data.Mapping;
import org.deri.cqels.data.MappingQuad;
import org.deri.cqels.engine.ExecContext;

import com.espertech.esper.client.EPStatement;
import com.espertech.esper.client.EventBean;
import com.espertech.esper.client.SafeIterator;
import com.hp.hpl.jena.sparql.core.Quad;

public class MappingIterOnQuadStatement extends MappingIter {
	SafeIterator<EventBean> safeItr;
	EPStatement stmt;
	Quad quad;
	public MappingIterOnQuadStatement(ExecContext context,EPStatement stmt,Quad quad) {
		this(context, stmt, quad, false);
	}
	
	public MappingIterOnQuadStatement(ExecContext context, EPStatement stmt, 
					Quad quad, boolean init) {
		super(context);
		this.stmt = stmt;
		this.quad = quad;
		if(init) {
			init();
		}
	}

	public void init() {
		safeItr = stmt.safeIterator();
	}
	
	@Override
	protected void closeIterator() {
		// TODO Auto-generated method stub
		if(safeItr != null) {
			safeItr.close();
		}
	}

	@Override
	protected boolean hasNextMapping() {
		if(safeItr != null) {
			return safeItr.hasNext();
		}
		return false;
	}

	@Override
	protected Mapping moveToNextMapping() {
		if(safeItr != null) {
			return  new MappingQuad(context,quad,(EnQuad) safeItr.next().getUnderlying());
		}
		return null;
	}

	@Override
	protected void requestCancel() {
		if(safeItr != null) {
			safeItr.close();
		}
	}

}
