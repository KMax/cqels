package org.deri.cqels.lang.cqels;

import java.util.Arrays;
import java.util.Collection;
import java.util.Set;

import org.deri.cqels.engine.Window;

import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.graph.Triple;
import com.hp.hpl.jena.sparql.algebra.Op;
import com.hp.hpl.jena.sparql.algebra.op.OpGraph;
import com.hp.hpl.jena.sparql.algebra.op.OpQuadPattern;
import com.hp.hpl.jena.sparql.algebra.op.OpTriple;
import com.hp.hpl.jena.sparql.core.BasicPattern;
import com.hp.hpl.jena.sparql.core.Var;

public class OpStream extends OpQuadPattern {
	Window window;
	public OpStream(Node node,BasicPattern pattern, Window window) {
		super(node, pattern);
		this.window=window;
	}
	
	public OpStream(Node node, Triple triple, Window window) {
		this(node, BasicPattern.wrap(Arrays.asList(triple)), window);
	}
	public Window getWindow() {
		return window;
	}
	/*
	public void vars(Set<Var> acc){
		addVar(acc,getGraphNode());
		addVarsFromTriple(acc, ((OpTriple)getSubOp()).getTriple());
	}
	
	private static void addVarsFromTriple(Collection<Var> acc, Triple t)
	{
	        addVar(acc, t.getSubject()) ;
	        addVar(acc, t.getPredicate()) ;
	        addVar(acc, t.getObject()) ;
	}
	private static void addVar(Collection<Var> acc, Node n)
    {
        if ( n == null )
            return ;
        
        if ( n.isVariable() )
            acc.add(Var.alloc(n)) ;
    }*/

}
