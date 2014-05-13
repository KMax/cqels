package org.deri.cqels.engine;

import com.hp.hpl.jena.sparql.algebra.Op;
/** 
 * This abstract class has characteristic of a unary router 
 * 
 * @author		Danh Le Phuoc
 * @author 		Chan Le Van
 * @organization DERI Galway, NUIG, Ireland  www.deri.ie
 * @email 	danh.lephuoc@deri.org
 * @email   chan.levan@deri.org
 * @see OpRouterBase
 */
public abstract class OpRouter1 extends OpRouterBase {
	protected OpRouter subRouter;
	public OpRouter1(ExecContext context, Op op, OpRouter subRouter) {
		super(context, op);
		this.subRouter = subRouter;
	}
	
	public void setSub(OpRouter subRouter) {
		this.subRouter = subRouter;
	}
	
	public OpRouter sub() { 
		return subRouter;
	}
}
