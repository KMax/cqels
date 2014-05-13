package org.deri.cqels.data;

import java.util.Iterator;

import org.deri.cqels.engine.ExecContext;
import com.hp.hpl.jena.sparql.core.Quad;
import com.hp.hpl.jena.sparql.core.Var;

public class MappingQuad extends MappingBase {
	
	Quad quad;
	EnQuad enQuad;
	public MappingQuad(ExecContext context,Quad quad, EnQuad enQuad) {
		super(context);
		this.quad=quad;
		this.enQuad=enQuad;
	}
	
	@Override
	public boolean isEmpty() {
		return quad.getGraph().isVariable()||quad.getSubject().isVariable()
				||quad.getPredicate().isVariable()||quad.getObject().isVariable();
	}
	
	@Override
	public void addParent(Mapping parent) {
		// TODO throw error : not allowed
	}
	
	@Override
	public boolean hasParent() { return false; }
	
	@Override
	public long get(Var var) {
		if(var.equals(quad.getGraph()))		return enQuad.getGID();
		if(var.equals(quad.getSubject()))  return enQuad.getSID();
		if(var.equals(quad.getPredicate()))  return enQuad.getPID();
		if(var.equals(quad.getObject()))  return enQuad.getOID();
		return -1;
	}
	
	@Override
	public int size() {
		int size=0;
		if(quad.getGraph().isVariable()) size++;
		if(quad.getSubject().isVariable()) size++;
		if(quad.getPredicate().isVariable()) size++;
		if(quad.getObject().isVariable()) size++;
		return size;
	}
	
	@Override
	public Iterator<Var> vars() { 
		return new  Iterator<Var>(){
	
			boolean gNRead=true,sNRead=true,pNRead=true,oNRead=true;
		
			public boolean hasNext() {
				if(gNRead&&quad.getGraph().isVariable()) return true;
				if(sNRead&&quad.getSubject().isVariable()) return true;
				if(pNRead&&quad.getPredicate().isVariable()) return true;
				if(oNRead&&quad.getObject().isVariable()) return true;
				return false;
			}
	
			public Var next() {
				if(gNRead&&quad.getGraph().isVariable()){
					gNRead=false;	return (Var)quad.getGraph();}
				if(sNRead&&quad.getSubject().isVariable()) {
					sNRead=false;  return (Var)quad.getSubject();}
				if(pNRead&&quad.getPredicate().isVariable()){
					pNRead=false; return (Var)quad.getPredicate(); }
				if(oNRead&&quad.getObject().isVariable()){
					oNRead=false; return (Var)quad.getObject();
				}
				return null;
			}
	
			public void remove() {
				// TODO Auto-generated method stub		
			}
		};
	}
}
