package org.deri.cqels.engine;

import com.hp.hpl.jena.sparql.algebra.Op;
/** 
 * This abstract class has characteristic of a binary router 
 * @author		Danh Le Phuoc
 * @author 		Chan Le Van
 * @organization DERI Galway, NUIG, Ireland  www.deri.ie
 * @email 	danh.lephuoc@deri.org
 * @email   chan.levan@deri.org
 */
public abstract class OpRouter2 extends OpRouterBase {
	OpRouter left, right;
	public OpRouter2(ExecContext context, Op op, OpRouter left, OpRouter right) {
		super(context, op);
		this.left = left;
		this.right = right;
	}
	
	public OpRouter left() { 
		return left;
	}
	
	public OpRouter right(){
		return right;
	}
}
