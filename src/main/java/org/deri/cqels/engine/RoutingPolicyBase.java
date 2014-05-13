package org.deri.cqels.engine;

import java.util.HashMap;

import org.deri.cqels.data.Mapping;

import com.hp.hpl.jena.query.Query;
/** 
 * This class is the base class to build an execution plan
 * 
 * @author		Danh Le Phuoc
 * @author 		Chan Le Van
 * @organization DERI Galway, NUIG, Ireland  www.deri.ie
 * @email 	danh.lephuoc@deri.org
 * @email   chan.levan@deri.org
 */
public abstract class RoutingPolicyBase implements RoutingPolicy {
    protected ExecContext context;
    protected HashMap<Integer,OpRouter> next;
    protected LogicCompiler compiler;
    public RoutingPolicyBase(ExecContext ctx) {
    	context = ctx;
    	next = new HashMap<Integer, OpRouter>();
    }
    public ExecContext getContext() {
    	return context;
    }
    protected abstract OpRouter generateRoutingPolicy(Query query);
    protected abstract OpRouter addRouter(OpRouter from, OpRouter newRouter);
}
