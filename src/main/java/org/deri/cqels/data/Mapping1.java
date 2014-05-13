package org.deri.cqels.data;

import org.deri.cqels.engine.ExecContext;

import com.hp.hpl.jena.sparql.core.Var;

public class Mapping1 extends MappingBase {
	
	Var var;
	long value;
	
	public Mapping1(ExecContext context, Var var, long value){
		super(context);
		this.var=var;
		this.value=value;
	}
	
	
	public Mapping1(ExecContext context, Var var, long value,Mapping parent){
		super(context,parent);
		this.var=var;
		this.value=value;
	}
	
	@Override
	public long get(Var var) {
		if(var.equals(var)) return value;
		return super.get(var);
	}
	
	@Override
	public boolean containsKey(Object key) {
		if(var.equals(key)) return true;
		return super.containsKey(key);
	}
	
	 
	@Override
	public boolean containsValue(Object value) {
		if(value.equals(new Long(this.value))) return true;
		return super.containsValue(value);
	}
	
	@Override
	public boolean isEmpty() { return true;}
	
	@Override
	public boolean isCompatible(Mapping mapping) {
		if(mapping.get(var)==-1) return super.isCompatible(mapping);
		return mapping.get(var)==value;
	}
	
	@Override
	public int size() {
		// TODO Auto-generated method stub
		return 1+super.size();
	}
}
