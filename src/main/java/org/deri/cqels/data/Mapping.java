package org.deri.cqels.data;

import java.util.Iterator;
import java.util.Map;

import org.deri.cqels.engine.ExecContext;
import org.deri.cqels.engine.OpRouter;

import com.hp.hpl.jena.sparql.algebra.Op;
import com.hp.hpl.jena.sparql.core.Var;

public interface Mapping extends Map<Var, Long>{
	

	public long get(Var var); 
	
	public void from(OpRouter router);
	
	public OpRouter from();
	
	public ExecContext getCtx();
	
	public Iterator<Var> vars();
	
	public void addParent(Mapping parent);
	
	public boolean hasParent();
	
	public boolean isCompatible(Mapping mapping);
}
