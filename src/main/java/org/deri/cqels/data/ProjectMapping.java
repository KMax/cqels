package org.deri.cqels.data;

import java.util.Iterator;
import java.util.List;

import org.deri.cqels.engine.ExecContext;

import com.hp.hpl.jena.sparql.core.Var;

public class ProjectMapping extends MappingBase {
	Mapping mapping;
	List<Var> vars;
	public ProjectMapping(ExecContext context, Mapping mapping, List<Var> vars) {
		super(context);
		this.mapping = mapping;
		this.vars = vars;
		//TODO check if mapping contains all variable in vars
	}
	
	@Override
	public long get(Var var) {
		if(vars.contains(var)) {
			return mapping.get(var);
		}
		return -1;
	}
	
	@Override
	public Iterator<Var> vars() {
		return vars.iterator();
	}
	
	@Override
	public boolean containsKey(Object key) {
		for(Var _var:vars)
			if(_var.equals(key)) return true;
		return false;
	}
	
	@Override
	public boolean containsValue(Object value) {
		//TODO don't allow data access
		return false;
	}
	
	@Override
	public boolean isCompatible(Mapping mapping) {
		for(Var var:vars)
			if(mapping.get(var)!=(this.mapping.get(var))) return false;
		return true;
	}
	
	@Override
	public boolean isEmpty() {
		
		return vars==null||vars.isEmpty();
	}
	
	@Override
	public int size() {
		// TODO Auto-generated method stub
		return vars!=null?vars.size():0;
	}
}
