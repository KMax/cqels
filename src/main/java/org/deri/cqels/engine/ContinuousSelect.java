package org.deri.cqels.engine;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;

import org.deri.cqels.data.Mapping;

import com.hp.hpl.jena.query.Query;
/** 
 * This class acts as a router standing in the root of the tree 
 * if the query is a select-type
 * @author		Danh Le Phuoc
 * @author 		Chan Le Van
 * @organization DERI Galway, NUIG, Ireland  www.deri.ie
 * @email 	danh.lephuoc@deri.org
 * @email   chan.levan@deri.org
 * @see OpRouter
 * @see OpRouterBase
 */
public class ContinuousSelect extends OpRouter1 {
	Query query;
	ArrayList<ContinuousListener> listerners;
	
	public ContinuousSelect(ExecContext context, Query query, OpRouter subRouter) {
		super(context, subRouter.getOp(), subRouter);
		listerners = new ArrayList<ContinuousListener>();
	}
	
	@Override
	public void route(Mapping mapping) {
		//System.out.println("out select "+mapping);
		for(ContinuousListener lit:listerners)
			lit.update(mapping);
	}
	
	public void register(ContinuousListener lit){ 
		listerners.add(lit); 
	}

	public void visit(RouterVisitor rv) {
		rv.visit(this);
		this.subRouter.visit(rv);
	}
}
