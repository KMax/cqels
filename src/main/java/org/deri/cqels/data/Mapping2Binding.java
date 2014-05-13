package org.deri.cqels.data;


import java.util.Iterator;

import org.deri.cqels.engine.ExecContext;

import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.sparql.core.Var;
import com.hp.hpl.jena.sparql.engine.binding.Binding;

public class Mapping2Binding implements Binding {
	Mapping mapping;
	ExecContext context;
	public Mapping2Binding(ExecContext context,Mapping mapping){
		this.mapping=mapping;
		this.context=context;
	}

	public Binding getParent() {
		// TODO Auto-generated method stub
		return null;
	}

	public void add(Var var, Node node) {
		// TODO Auto-generated method stub
		
	}

	public Iterator<Var> vars() {
		// TODO Auto-generated method stub
		return mapping.vars();
	}

	public boolean contains(Var var) {
		// TODO Auto-generated method stub
		return mapping.containsKey(var);
	}

	public Node get(Var var) {
		// TODO Auto-generated method stub
		//System.out.print("get Var "+var +"=");
		//System.out.println(" "+	context.engine().decode(mapping.get(var)));
		return context.engine().decode(mapping.get(var));
	}

	public int size() {
		// TODO Auto-generated method stub
		return mapping.size();
	}

	public boolean isEmpty() {
		// TODO Auto-generated method stub
		return false;
	}

	public void addAll(Binding key) {
		// TODO Auto-generated method stub
		
	}
	
	

}
