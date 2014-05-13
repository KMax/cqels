package org.deri.cqels.engine;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;

import org.deri.cqels.data.Mapping;
import org.deri.cqels.engine.iterator.MappingIterCursorAll;
import org.deri.cqels.engine.iterator.MappingIterCursorByKey;
import org.deri.cqels.engine.iterator.MappingIterCursorByRangeKey;
import org.deri.cqels.engine.iterator.MappingIterator;
import org.deri.cqels.engine.iterator.NullMappingIter;

import com.hp.hpl.jena.rdf.model.ModelFactory;
import com.hp.hpl.jena.sparql.algebra.Op;
import com.hp.hpl.jena.sparql.core.DatasetGraph;
import com.hp.hpl.jena.sparql.core.Var;
import com.hp.hpl.jena.sparql.engine.QueryIterator;
import com.hp.hpl.jena.sparql.engine.binding.Binding;

import com.sleepycat.bind.tuple.TupleInput;
import com.sleepycat.bind.tuple.TupleOutput;
import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseConfig;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.SecondaryConfig;
import com.sleepycat.je.SecondaryDatabase;
import com.sleepycat.je.SecondaryKeyCreator;
/**
 * This class implements a router that its data mapping buffer is static
 * @author		Danh Le Phuoc
 * @author 		Chan Le Van
 * @organization DERI Galway, NUIG, Ireland  www.deri.ie
 * @email 	danh.lephuoc@deri.org
 * @email   chan.levan@deri.org
 * @see OpRouterBase
 */
public class BDBGraphPatternRouter extends OpRouterBase {
	ArrayList<String> idxDescs;
	HashMap<Var,Integer> var2Idx;
	ArrayList<Var> vars;
	ArrayList<ArrayList<Integer>> indexes;
	Database[] idxDbs; 
	static final DatabaseEntry EMPTYDATA = new DatabaseEntry(new byte[0]);
	Database mainDB;
	
	/**
	 *The constructor
	 *@param context
	 *@param op
	 */
	public BDBGraphPatternRouter(ExecContext context, Op op) {
		super(context, op);
		//this.op=op;
		//ModelFactory.createModelForGraph(context.getDataset().getDefaultGraph()).write(System.out);
		init(context.getDataset());
	}
	
	/**
	 *The constructor
	 *@param context
	 *@param op
	 *@param ds
	 */
	public BDBGraphPatternRouter(ExecContext context, Op op, DatasetGraph ds) {
		super(context, op);
		//this.op=op;
		//ModelFactory.createModelForGraph(ds.getDefaultGraph()).write(System.out);
		init(ds);
	}
	
	private void init(DatasetGraph ds) {
		DatabaseConfig dbConfig = new DatabaseConfig();
		dbConfig.setAllowCreate(true);
		mainDB = context.env().openDatabase(null, "pri_cache_" + getId(), dbConfig);
		//System.out.println(op);
		//materialize the query result here;
		QueryIterator itr = context.loadGraphPattern(op, ds);
		if(itr.hasNext()) {
			      //System.out.println("has Next");
			Binding binding = itr.next();
			vars = new ArrayList<Var>();
			var2Idx = new HashMap<Var, Integer>();
			
			for(Iterator<Var> itrV = binding.vars(); itrV.hasNext(); ) {
				Var var = itrV.next();
				if(!var2Idx.containsKey(var)) {
					vars.add(var);
					var2Idx.put(var, vars.size() - 1);
				}
			}
			int lastIdx = vars.size() - 1;
			
			indexes = new ArrayList<ArrayList<Integer>>();
			ArrayList<Integer> first = new ArrayList<Integer>();
			first.add(new Integer(lastIdx));
			
			indexes.add(first);
			for(int i = 1; i < vars.size(); i++) {
				int size = indexes.size();
				for(int k = 0; k < size; k++) {
					ArrayList<Integer> next = new ArrayList<Integer>(indexes.get(k));
					next.add(new Integer(lastIdx - i));
					indexes.add(next);
				}
			}
			idxDbs = new Database[indexes.size()];
			for( int i = 0; i < indexes.size() - 1; i++) {
				SecondaryConfig idxConfig = new SecondaryConfig();
				idxConfig.setAllowCreate(true);
				idxConfig.setSortedDuplicates(true);
				idxConfig.setKeyCreator(new KeyGenerator(indexes.get(i), vars.size()));
				
				idxDbs[i] = context.env().openSecondaryDatabase(null, 
										  "idx_cache_" + getId() + "-" + i, mainDB, idxConfig);
			}
			idxDbs[indexes.size() - 1] = mainDB;
			addBinding2Cache(binding);
			while(itr.hasNext()) {
				addBinding2Cache(itr.next());
			}
		}
			
	}
	
	private void addBinding2Cache(Binding binding) {
		//System.out.println(binding);
		TupleOutput output = new TupleOutput();
		for(int i = 0; i < vars.size(); i++) { 
			output.writeLong(context.engine().encode(binding.get(vars.get(i))));
		}
		DatabaseEntry tmp = new DatabaseEntry(output.getBufferBytes());
		mainDB.put(null, tmp, tmp);
	}
	
	/**
	 *@return the data buffer of dataset
	 */
	@Override
	public MappingIterator getBuff() { 
		return new MappingIterCursorAll(context, mainDB, vars);
	}
	

	@Override
	public MappingIterator searchBuff4Match(Mapping mapping) {
		if (vars == null) {
			return NullMappingIter.instance();
		}
		 
		//System.out.println("search " +mapping);
		ArrayList<Integer> idxMask = new ArrayList<Integer>();
		for(int i = 0; i < vars.size(); i++) {
			if(mapping.containsKey(vars.get(i))) {
				idxMask.add(i);
			}
		}
		//System.out.println(idxMask.size());
		int idx = -1, weight = 0;
		for(int i = 0; i < indexes.size(); i++) {
			int count = 0;
			int p = indexes.get(i).size() - 1;
			for(int l = 0; l < idxMask.size() && p >= 0; l++) {
		//	   System.out.println(idxMask.get(l)+ "--"+indexes.get(i).get(p) + " "+(idxMask.get(l).equals(indexes.get(i).get(p))));	
			   if(!idxMask.get(l).equals(indexes.get(i).get(p--))) {
				   break;	
			   }
			   count++;
			}
		//	System.out.println("count "+count);
			if(count > weight) { 
				weight = count; 
				idx = i;
			}
		}
	//	System.out.println("weight" +weight);
		if(weight > 0) {
			if(weight < indexes.get(idx).size()) {
				TupleOutput out = new TupleOutput();
				for(int i = 0; i < idxMask.size(); i++) {
					out.writeLong(mapping.get(vars.get(idxMask.get(i))));
			    }
				//add O mask for the lowest range key
				for(int i = 0; i < indexes.get(idx).size() - weight; i++) {
					out.writeLong(0);
				}
				//System.out.println("return MappingIterCursorByRangeKey");
				return new MappingIterCursorByRangeKey(context, idxDbs[idx], 
									new DatabaseEntry(out.getBufferBytes()), 
													  mapping, vars, weight);
			}
			else {
				TupleOutput out = new TupleOutput();
				for(int i = 0; i < idxMask.size(); i++) {
					out.writeLong(mapping.get(vars.get(idxMask.get(i))));
				}
				return new MappingIterCursorByKey(context, idxDbs[idx], 
							   new DatabaseEntry(out.getBufferBytes()), 
												 mapping, vars);
			}
		}
		return NullMappingIter.instance();
	}
	
	/**
	 * @return return iterator
	 */
	public MappingIterator iterator() {
		return new MappingIterCursorAll(context, mainDB, vars);
	}
	
	class KeyGenerator implements SecondaryKeyCreator {
		ArrayList<Integer> idxList; int size;
		public KeyGenerator(ArrayList<Integer> idxList, int size) {
			this.idxList = idxList;
			this.size = size;
		}

		/**
		 * @param secondary
		 * @param key
		 * @param data
		 * @param result
		 */
		public boolean createSecondaryKey(SecondaryDatabase secondary,
				DatabaseEntry key, DatabaseEntry data, DatabaseEntry result) {
			TupleInput dataInput = new TupleInput(data.getData());
			TupleOutput indexKeyOutput = new TupleOutput();
			int p = idxList.size() - 1;
			for(int l = 0; l < size && p >= 0; l++) {
			   long tmp = dataInput.readLong();
				   if(l == idxList.get(p)) {
					   indexKeyOutput.writeLong(tmp);
					   p--;
				   }
			}
			result.setData(indexKeyOutput.getBufferBytes());
			return true;
		}
	}

	public void visit(RouterVisitor rv) {
		rv.visit(this);
	}
}
