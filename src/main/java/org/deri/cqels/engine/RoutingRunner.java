package org.deri.cqels.engine;

import org.deri.cqels.data.Mapping;
import org.deri.cqels.engine.iterator.MappingIterator;

import com.hp.hpl.jena.sparql.algebra.Op;

public class RoutingRunner extends Thread {
	
	ExecContext context;
	MappingIterator itr;
	OpRouterBase router;
	public RoutingRunner (ExecContext context,MappingIterator itr,OpRouterBase router){
		this.context=context;
		this.itr=itr;
		this.router=router;
	}

	public void run() {	
		//System.out.println("start send thread "+this.toString());
		//System.out.println(mapping.getCtx().plan());
		//TODO: get plan via planner
		
		while (itr.hasNext()) {
			
			Mapping _mapping=itr.next();
			router._route( _mapping);
		}
		//System.out.println("end of thread "+this.toString());
		itr.close();
		try {
			this.finalize();
		} catch (Throwable e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		//System.out.println("end thread "+this.getId());
	}
	
}
