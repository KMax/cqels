package org.deri.cqels.data;

import java.util.Iterator;

import org.deri.cqels.engine.ExecContext;

import com.hp.hpl.jena.sparql.core.Var;
import com.hp.hpl.jena.sparql.engine.binding.Binding;

public class Binding2Mapping extends MappingBase {
	Binding binding;
	public Binding2Mapping(ExecContext context, Binding binding) {
		super(context);
		this.binding=binding;
		//System.out.println("binding "+binding);
	}
	
	@Override
	public Iterator<Var> vars() {
		// TODO Auto-generated method stub
		return binding.vars();
	}
	
	@Override
	public long get(Var var) {
		if(binding.contains(var))
			return context.engine().encode(binding.get(var));
		return -1;
	}
	
	@Override
	public boolean containsKey(Object key) {
		// TODO Auto-generated method stub
		return binding.contains((Var)key);
	}
	
	
	@Override
	public int size() {
		// TODO Auto-generated method stub
		return binding.size();
	}
}
