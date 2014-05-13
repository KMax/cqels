package org.deri.cqels.data;

import java.util.Iterator;

import org.deri.cqels.engine.ExecContext;
import org.openjena.atlas.iterator.IteratorConcat;

import com.hp.hpl.jena.sparql.core.Var;
import com.hp.hpl.jena.sparql.core.VarExprList;
import com.hp.hpl.jena.sparql.expr.ExprVar;

public class ExtendMapping extends MappingBase {
	Mapping mapping;
	VarExprList exprs;
	public ExtendMapping(ExecContext context,Mapping mapping,VarExprList exprs) {
		super(context);
		this.mapping=mapping;
		this.exprs=exprs;
		//System.out.println("extend"+exprs);
	}
	
	public ExtendMapping(ExecContext context,Mapping mapping,Mapping parent) {
		super(context,parent);
		this.mapping=mapping;
	}
	@Override
	public long get(Var var) {
		//System.out.print("get "+ var +"=");
		//System.out.println(mapping.get(var));
		if(mapping.get(var)!=-1)
			return mapping.get(var);
		if(exprs.contains(var)){
			if(exprs.getExpr(var) instanceof ExprVar){
				//System.out.println("get ex "+((ExprVar)exprs.getExpr(var)).asVar());
				return mapping.get(((ExprVar)exprs.getExpr(var)).asVar());
			}
			else System.out.println("As with expression is not supported "+exprs.getExpr(var));
		}
		return super.get(var);
	}
	
	@Override
	public Iterator<Var> vars() {
		if(hasParent())
			return IteratorConcat.concat(IteratorConcat.concat(mapping.vars(),exprs.getVars().iterator()),super.vars());
		return IteratorConcat.concat(mapping.vars(),exprs.getVars().iterator());
	}
	@Override
	public boolean containsKey(Object key) {
		if(mapping.containsKey(key)) return true;
		if(exprs.contains((Var)key)) return true;
		return super.containsKey(key);
	}
	
	
	@Override
	public boolean containsValue(Object value) {
		if(mapping.containsValue(value)) return true;
		return super.containsValue(value);
	}
	
	@Override
	public boolean isCompatible(Mapping mapping) {
		//TODO : need to be checked
		if(!mapping.isCompatible(this)) return false;
		return super.isCompatible(mapping);
	}
	
	@Override
	public boolean isEmpty() {
		if(mapping.isEmpty()) return true;
		return super.isEmpty();
	}
	
	@Override
	public int size() {
		// TODO Auto-generated method stub
		return mapping.size()+super.size()+exprs.size();
	}
}
