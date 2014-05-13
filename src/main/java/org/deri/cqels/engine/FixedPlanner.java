package org.deri.cqels.engine;

import java.util.HashMap;

import org.deri.cqels.data.EnQuad;

import com.hp.hpl.jena.sparql.algebra.Op;
import com.hp.hpl.jena.sparql.algebra.OpVisitorByType;
import com.hp.hpl.jena.sparql.algebra.op.Op0;
import com.hp.hpl.jena.sparql.algebra.op.Op1;
import com.hp.hpl.jena.sparql.algebra.op.Op2;
import com.hp.hpl.jena.sparql.algebra.op.OpExt;
import com.hp.hpl.jena.sparql.algebra.op.OpN;

public class FixedPlanner {
	ExecContext context;
	Op op;
	HashMap<Op, Integer> plan;
	public FixedPlanner(ExecContext context,Op op){
		this.context=context;
		this.op=op;
		createFixedPlan();
	}
	private void createFixedPlan(){
		plan= new HashMap<Op, Integer>();
		op.visit(new OpVisitorByType() {
			
			@Override
			protected void visitN(OpN _op) {
				for(int i=0;i<_op.size();i++)
					_visit(_op.get(i), _op);
			}
			
			@Override
			protected void visitExt(OpExt op) {
				// TODO check with windows ...
				
			}
			
			@Override
			protected void visit2(Op2 _op) {
				_visit(_op.getLeft(), _op);
				_visit(_op.getRight(), _op);
				
			}
			
			@Override
			protected void visit1(Op1 _op) {
				_visit(_op.getSubOp(), _op);
			}
			
			@Override
			protected void visit0(Op0 _op) {
				// TODO do nothing?
				
			}
			
			protected void _visit(Op child,Op parent){
				//plan.put(child, context.op2Id(parent));
				child.visit(this);
			}
		});
		plan.put(op, 0);
		System.out.println(" Plan "+ plan);
	}
	
	public HashMap<Op, Integer> getPlan(Op op, EnQuad quad) {
		return plan;
	}

}
