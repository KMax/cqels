package org.deri.cqels.engine;

import java.lang.reflect.Method;
import java.util.ArrayList;

import org.deri.cqels.data.Mapping;
import org.deri.cqels.data.MappingWrapped;
import org.deri.cqels.engine.iterator.MappingIterator;

import com.hp.hpl.jena.query.Query;
import com.hp.hpl.jena.sparql.algebra.Op;

public class ThroughRouter extends OpRouterBase {
	ArrayList<OpRouter> dataflows;
	public ThroughRouter(ExecContext context, ArrayList<OpRouter> dataflows) {
		super(context, dataflows.get(0).getOp());
		this.dataflows = dataflows;
	}

	@Override
	public void route(Mapping mapping) {
		_route(new MappingWrapped(context, mapping));
	}
	
	@Override
	public MappingIterator searchBuff4Match(Mapping mapping) {
		return dataflows.get(0).searchBuff4Match(mapping);
	}
	
	@Override
	public MappingIterator getBuff() {
		return dataflows.get(0).getBuff();
	}

	public void visit(RouterVisitor rv) {
		rv.visit(this);
		OpRouter router = dataflows.get(0);
		try {
			Method method = router.getClass().getDeclaredMethod("visit", RouterVisitor.class);
			method.invoke(router, rv);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
