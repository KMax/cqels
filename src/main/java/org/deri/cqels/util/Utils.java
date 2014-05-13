package org.deri.cqels.util;

import java.util.ArrayList;
import java.util.HashMap;

import org.deri.cqels.data.HashMapping;
import org.deri.cqels.data.Mapping;
import org.deri.cqels.data.MappingWrapped;
import org.deri.cqels.engine.ExecContext;

import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.sparql.core.Quad;
import com.hp.hpl.jena.sparql.core.Var;
import com.hp.hpl.jena.sparql.engine.binding.Binding;
import com.sleepycat.bind.tuple.TupleInput;
import com.sleepycat.je.DatabaseEntry;

public class Utils {

	public static void _addBinding(Node node, HashMap<Var, Long> hMap,Mapping mapping, TupleInput input){
		if(node.isVariable()&&(!mapping.containsKey((Var)node)))
			hMap.put((Var)node,input.readLong());
	}
	
	
	public static ArrayList<Var>  quad2Vars(Quad quad){
		ArrayList<Var> vars=new ArrayList<Var>();
		if(quad.getGraph().isVariable()) vars.add((Var)quad.getGraph());
		if(quad.getSubject().isVariable()) vars.add((Var)quad.getSubject());
		if(quad.getPredicate().isVariable()) vars.add((Var)quad.getPredicate());
		if(quad.getObject().isVariable()) vars.add((Var)quad.getObject());
		return vars;
	}
	 
	public static Mapping data2Mapping(ExecContext context,DatabaseEntry data,Mapping mapping,ArrayList<Var> vars){
		return new MappingWrapped(context, data2MappingFilter(context, data, mapping, vars),mapping );
	}
	
	public static Mapping data2MappingFilter(ExecContext context,DatabaseEntry data,Mapping mapping, ArrayList<Var> vars){
		HashMap<Var, Long> hMap=new HashMap<Var, Long>(); 
		TupleInput input= new TupleInput(data.getData());	
		for(int i=0;i<vars.size();i++){
			long tmp=input.readLong();
			if(!mapping.containsKey(vars.get(i))) hMap.put(vars.get(i), tmp);
		}
		return new HashMapping(context, hMap);
	}
	
	public static Mapping data2Mapping(ExecContext context,DatabaseEntry data, ArrayList<Var> vars){
		HashMap<Var, Long> hMap=new HashMap<Var, Long>(); 
		TupleInput input= new TupleInput(data.getData());	
		for(int i=0;i<vars.size();i++)
			hMap.put(vars.get(i), input.readLong());
		
		return new HashMapping(context, hMap);
	}

	public static boolean outRange(DatabaseEntry data, DatabaseEntry range,int idxLen){
		TupleInput in1=new TupleInput(data.getData());
		TupleInput in2=new TupleInput(range.getData());
		for(int i=0;i<idxLen;i++)
			if(in1.readLong()!=in2.readLong()) return true;
		return false;
	}

}
