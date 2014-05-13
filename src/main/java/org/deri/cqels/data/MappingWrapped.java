package org.deri.cqels.data;

import com.hp.hpl.jena.sparql.core.Var;
import java.util.Iterator;
import org.deri.cqels.engine.ExecContext;
import org.openjena.atlas.iterator.IteratorConcat;

public class MappingWrapped extends MappingBase {
	Mapping mapping;
	public MappingWrapped(ExecContext context, Mapping mapping) {
		super(context);
		this.mapping = mapping;
		// TODO Auto-generated constructor stub
	}
	
	public MappingWrapped(ExecContext context, Mapping mapping, Mapping parent) {
		super(context, parent);
		this.mapping = mapping;
	}
	@Override
	public long get(Var var) {
		if(mapping.get(var) != -1)
			return mapping.get(var);
		return super.get(var);
	}
	
	@Override
	public Iterator<Var> vars() {
		if(hasParent())
			return IteratorConcat.concat(mapping.vars(),super.vars());
		return mapping.vars();
	}
	@Override
	public boolean containsKey(Object key) {
		if(mapping.containsKey(key)) { 
			return true;
		}
		return super.containsKey(key);
	}
	
	@Override
	public boolean containsValue(Object value) {
		if(containsValue(value)) {
			return true;
		}
		return super.containsValue(value);
	}
	
	@Override
	public boolean isCompatible(Mapping mapping) {
		if(!this.mapping.isCompatible(mapping)) {
			return false;
		}
		return super.isCompatible(mapping);
	}
	
	@Override
	public boolean isEmpty() {
		if(mapping.isEmpty()) {
			return true;
		}
		return super.isEmpty();
	}
	
	@Override
	public int size() {
		// TODO Auto-generated method stub
		return mapping.size() + super.size();
	}
}
