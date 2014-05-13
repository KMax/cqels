package org.deri.cqels.data;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import org.deri.cqels.engine.ExecContext;
import org.deri.cqels.engine.OpRouter;

import com.hp.hpl.jena.sparql.core.Var;
import com.hp.hpl.jena.util.iterator.NullIterator;

public  abstract class MappingBase implements Mapping {
	static int  count = 0;
	protected ExecContext context;
	ArrayList<Mapping> parents;
	int opID;
	OpRouter from;
	public MappingBase(ExecContext context) {
		this.context = context;
		//this.opID=allocID();
	}
	
	public MappingBase(ExecContext context, Mapping parent) {
		this(context);
	    addParent(parent);
			
	}
	
	/*protected static synchronized int allocID(){
		return count++;
	}*/
	
	public boolean hasParent() {
		return (parents != null) && (!parents.isEmpty());
	}
	
	
	public ExecContext getCtx() { return context; }
	
	public OpRouter from(){ return from;}
	public void from(OpRouter from){ this.from = from; }


	public void clear() {
		// TODO Auto-generated method stub		
	}
	
	public void addParent(Mapping parent){
		if(parents==null) parents=new ArrayList<Mapping>();
		parents.add(parent);
	}
	
	public boolean containsKey(Object key) {
		if(parents!=null)
			for(Mapping parent:parents)
				if (parent.containsKey(key)) return true;
			//for(int i=0;i<parents.size();i++)
				//if (parents.get(i).containsKey(key)) return true;
		return false;	
	}

	public boolean containsValue(Object value) {
		
		if(parents!=null)
			for(Mapping parent:parents)
				if (parent.containsValue(value)) return true;
		//for(int i=0;i<parents.size();i++)
		//if (parents.get(i).containsValue(value)) return true;
		return false;	
		
	}
	
	public Iterator<Var> vars(){
		if(hasParent()) return new VarIteratorArrayList(parents);
		return NullIterator.instance();
	}
	
	public Set<java.util.Map.Entry<Var, Long>> entrySet() {
		// TODO Auto-generated method stub
		return null;
	}

	public long get(Var var){
		if(parents!=null)
			for(Mapping parent:parents)
				if(parent.get(var)!=-1l) return parent.get(var);
			//for(int i=0;i<parents.size();i++)
				//if(parents.get(i).get(var)!=-1l) return parents.get(i).get(var);
		return -1; 
	}
	
	public Long get(Object key) {
		long value=-1;
		if(key instanceof Var)
			value= get((Var)key);
		return (value==-1)?null:value;
	}

	public boolean isEmpty() {
		// TODO Auto-generated method stub
		return true;
	}

	public Set<Var> keySet() {
		// TODO throw error if called
		return null;
	}

	public Long put(Var key, Long value) {
		// TODO throw error if called
		return null;
	}

	public void putAll(Map<? extends Var, ? extends Long> m) {
		// TODO throw error if called
		
	}

	public Long remove(Object key) {
		// TODO throw error if called
		return null;
	}

	public int size() {
		
		//TODO: distinct vars
		
		int size=0;
		if(hasParent())
			for(Mapping parent:parents)
				size+=parent.size();
			//for(int i=0;i<parents.size();i++)
				//size+=parents.get(i).size();
		return size;
	}

	public Collection<Long> values() {
		// TODO throw errof if called
		return null;
	}
	
	public boolean isCompatible(Mapping mapping){
		if(parents!=null)
			for(Mapping parent:parents)
				if(!parent.isCompatible(mapping)) return false;
			//for(int i=0;i<parents.size();)
				//if(!parents.get(i).isCompatible(mapping)) return false;
		return true;
	}
	
	public class VarIteratorArrayList implements Iterator<Var>{
		ArrayList<Mapping> mappings;
		int idx; Iterator<Var> cItr;
		public VarIteratorArrayList(ArrayList<Mapping> mappings ){
			this.mappings=mappings;
			idx=0;
			if(mappings.size()>0) cItr=mappings.get(0).vars();
		}
		
		public boolean hasNext() {
			
			if(cItr.hasNext()) return true;
			if (mappings.size()<=++idx) return false;
			cItr=mappings.get(idx).vars();			
			return hasNext();
		}

		public Var next() {
			if(hasNext()) return cItr.next();
			return null;
		}

		public void remove() {
			// TODO Auto-generated method stub
			
		}
		
	}
	@Override
	public String toString() {
		//System.out.println(this.getClass());
		String st=this.getClass()+"[";
		for(Iterator<Var> itr=vars();itr.hasNext();){
			Var var=itr.next();
			st+=" "+var+"="+get(var);
		}
		st+="]";
		return st;
	}
	
	
}
