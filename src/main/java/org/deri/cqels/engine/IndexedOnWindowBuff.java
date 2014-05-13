package org.deri.cqels.engine;

import java.util.ArrayList;
import java.util.concurrent.TimeUnit;

import org.deri.cqels.data.EnQuad;
import org.deri.cqels.data.Mapping;
import org.deri.cqels.engine.iterator.MappingIterCursorAll;
import org.deri.cqels.engine.iterator.MappingIterCursorByKey;
import org.deri.cqels.engine.iterator.MappingIterCursorByRangeKey;
import org.deri.cqels.engine.iterator.MappingIterator;
import org.deri.cqels.engine.iterator.NullMappingIter;
import org.deri.cqels.util.Utils;

import com.hp.hpl.jena.sparql.algebra.Op;
import com.hp.hpl.jena.sparql.core.Quad;
import com.hp.hpl.jena.sparql.core.Var;
import com.sleepycat.bind.tuple.LongBinding;
import com.sleepycat.bind.tuple.TupleInput;
import com.sleepycat.bind.tuple.TupleOutput;
import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseConfig;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.SecondaryConfig;
import com.sleepycat.je.SecondaryDatabase;
import com.sleepycat.je.SecondaryKeyCreator;
/** 
 * @author		Danh Le Phuoc
 * @organization DERI Galway, NUIG, Ireland  www.deri.ie
 * @email 	danh.lephuoc@deri.org
 */
public class IndexedOnWindowBuff {
	ExecContext context;
	Quad quad;
	Database buff;
	Op op;
	OpRouter router;
	ArrayList<Var> vars;
	ArrayList<ArrayList<Integer>> indexes;
	SecondaryDatabase[] idxDbs; 
	Window w;
	public IndexedOnWindowBuff(ExecContext  context,Quad quad, OpRouter router, Window w){
		this.context = context;
		this.quad = quad;
		this.router = router;
		this.op = router.getOp();
		init();
		this.w = w;
		//if(w==null) System.out.println("null "+quad);
		w.setBuff(buff);
	}
	
	public void init() {
		DatabaseConfig dbConfig = new DatabaseConfig();
		dbConfig.setAllowCreate(true);
		dbConfig.setTemporary(true);
	    //dbConfig.setTransactional(true);
		buff = context.env().openDatabase(null, "pri_synopsis_" + router.getId(), dbConfig);
		//System.out.println("ttrans "+dbConfig.getTransactional());
		initIndexes();
	}
	

	public void initIndexes() {
		vars = Utils.quad2Vars(quad);
		int lastIdx = vars.size() - 1;
		indexes = new ArrayList<ArrayList<Integer>>();
		ArrayList<Integer> first = new ArrayList<Integer>();
		first.add(new Integer(lastIdx));
		
		indexes.add(first);
		for(int i = 1; i < vars.size(); i++) {
			int size = indexes.size();
			for(int k = 0; k < size; k++) {
				ArrayList<Integer> next = new ArrayList<Integer>(indexes.get(k));
				next.add(new Integer(lastIdx-i));
				indexes.add(next);
			}
		}
		idxDbs = new SecondaryDatabase[indexes.size()];
		for( int i = 0; i < indexes.size(); i++) {
			SecondaryConfig idxConfig = new SecondaryConfig();
			idxConfig.setAllowCreate(true);
			idxConfig.setTemporary(true);
			//idxConfig.setTransactional(true);
			idxConfig.setSortedDuplicates(true);
			idxConfig.setKeyCreator(new KeyGenerator(indexes.get(i),vars.size()));
			
			idxDbs[i] = context.env().openSecondaryDatabase(null, "idx_synopsis_"+router.getId()+ "-" + i, buff, idxConfig);
			
		}
	}
	
	public void add(EnQuad enQuad) {
		DatabaseEntry key = new DatabaseEntry();
		LongBinding.longToEntry(enQuad.time(), key);
		w.reportLatestTime(enQuad.time());
		TupleOutput out = new TupleOutput();
		
		if(quad.getGraph().isVariable()) {
			out.writeLong(enQuad.getGID());
		}
		if(quad.getSubject().isVariable()) {
			out.writeLong(enQuad.getSID());
		}
		if(quad.getPredicate().isVariable()) {
			out.writeLong(enQuad.getPID());
		}
		if(quad.getObject().isVariable()) {
			out.writeLong(enQuad.getOID());
		}
		buff.put(null, key, new DatabaseEntry(out.getBufferBytes()));
	}
	
	public MappingIterator search4MatchMapping(Mapping mapping) {
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
			if(weight < indexes.get(idx).size())
			{
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
			else{
				
				TupleOutput out = new TupleOutput();
				for(int i = 0; i < idxMask.size(); i++) {
					out.writeLong(mapping.get(vars.get(idxMask.get(i))));
					//System.out.println(idxMask.get(i)+" "+vars.get(idxMask.get(i))+"="+mapping.get(vars.get(idxMask.get(i))));
				}
				//System.out.println("return MappingIterCursorByKey");
				return new MappingIterCursorByKey(context, idxDbs[idx], 
												  new DatabaseEntry(out.getBufferBytes()), 
												  					mapping, vars);
			}
		}
		return NullMappingIter.instance();
	}
	
	public MappingIterator iterator() {
		return new MappingIterCursorAll(context, buff, vars);
	}
	
	class KeyGenerator implements SecondaryKeyCreator {
		
		ArrayList<Integer> idxList; int size;
		public KeyGenerator(ArrayList<Integer> idxList, int size) {
			//System.out.println("build index "+ idxList.size());
			this.idxList = idxList;
			this.size = size;
		}

		public boolean createSecondaryKey(SecondaryDatabase secondary,
				DatabaseEntry key, DatabaseEntry data, DatabaseEntry result) {
			TupleInput dataInput = new TupleInput(data.getData());
			TupleOutput indexKeyOutput = new TupleOutput();
			int p = idxList.size() - 1;
			//System.out.println("key lenght"+idxList.size());
			for(int l = 0; l < size && p >= 0; l++) {
			   long tmp = dataInput.readLong();
			   //while(p>=0)
				   if(l == idxList.get(p)) {
					  // System.out.print("key "+l+" = "+tmp +" ");
					   indexKeyOutput.writeLong(tmp);
					   p--;
				   }
			}
			 //System.out.println("");
			result.setData(indexKeyOutput.getBufferBytes());
			return true;
		}
		
	}
}
