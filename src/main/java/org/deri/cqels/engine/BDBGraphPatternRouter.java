package org.deri.cqels.engine;

import com.hp.hpl.jena.graph.Graph;
import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.graph.Triple;
import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.ModelFactory;
import com.hp.hpl.jena.sparql.algebra.Op;
import com.hp.hpl.jena.sparql.core.DatasetGraph;
import com.hp.hpl.jena.sparql.core.Var;
import com.hp.hpl.jena.sparql.engine.QueryIterator;
import com.hp.hpl.jena.sparql.engine.binding.Binding;
import com.hp.hpl.jena.sparql.sse.SSE;
import com.sleepycat.bind.tuple.TupleInput;
import com.sleepycat.bind.tuple.TupleOutput;
import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseConfig;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.SecondaryConfig;
import com.sleepycat.je.SecondaryDatabase;
import com.sleepycat.je.SecondaryKeyCreator;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.DefaultHttpClient;
import org.deri.cqels.data.Mapping;
import org.deri.cqels.engine.iterator.MappingIterCursorAll;
import org.deri.cqels.engine.iterator.MappingIterCursorByKey;
import org.deri.cqels.engine.iterator.MappingIterCursorByRangeKey;
import org.deri.cqels.engine.iterator.MappingIterator;
import org.deri.cqels.engine.iterator.NullMappingIter;
import org.linkeddatafragments.model.LinkedDataFragmentGraph;
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
	
    /**
     * The function determines whether the remote url is TPF.
     *
     * @param url url adress of SPARQL endpoint or TriplePatternFragments server
     * @see https://lists.w3.org/Archives/Public/public-hydra/2015Aug/0040.html
     */
    private boolean isTPFServer(String url) {
        HttpClient client = new DefaultHttpClient();
        HttpGet get = new HttpGet(url);
        try {
            get.setHeader("Accept", "text/turtle");
            HttpResponse response = client.execute(get);
            Model mod = ModelFactory.createDefaultModel();            
            mod.read(response.getEntity().getContent(), null, "TURTLE");
            Graph graph = mod.getGraph();
            
            Triple triple0 = new Triple(Node.ANY, 
                    Node.createURI("http://www.w3.org/1999/02/22-rdf-syntax-ns#type"), 
                    Node.createURI("http://www.w3.org/ns/hydra/core#Collection"));
            Triple triple1 = new Triple(Node.ANY, 
                    Node.createURI("http://www.w3.org/1999/02/22-rdf-syntax-ns#type"), 
                    Node.createURI("http://rdfs.org/ns/void#Dataset"));
            Triple triple2 = new Triple(Node.ANY, 
                    Node.createURI("http://rdfs.org/ns/void#subset"), 
                    Node.createURI(url));
            Triple triple3 = new Triple(Node.ANY, 
                    Node.createURI("http://www.w3.org/ns/hydra/core#search"), 
                    Node.createVariable("_:triplePattern"));
            Triple triple4 = new Triple(Node.createVariable("_:triplePattern"), 
                    Node.createURI("http://www.w3.org/ns/hydra/core#mapping"), 
                    Node.createVariable("_:subject"));
            Triple triple5 = new Triple(Node.createVariable("_:triplePattern"), 
                    Node.createURI("http://www.w3.org/ns/hydra/core#mapping"), 
                    Node.createVariable("_:predicate"));
            Triple triple6 = new Triple(Node.createVariable("_:triplePattern"), 
                    Node.createURI("http://www.w3.org/ns/hydra/core#mapping"), 
                    Node.createVariable("_:object"));
            Triple triple7 = new Triple(Node.createVariable("_:subject"), 
                    Node.createURI("http://www.w3.org/ns/hydra/core#property"), 
                    Node.createURI("http://www.w3.org/1999/02/22-rdf-syntax-ns#subject"));
            Triple triple8 = new Triple(Node.createVariable("_:predicate"), 
                    Node.createURI("http://www.w3.org/ns/hydra/core#property"), 
                    Node.createURI("http://www.w3.org/1999/02/22-rdf-syntax-ns#predicate"));
            Triple triple9 = new Triple(Node.createVariable("_:object"), 
                    Node.createURI("http://www.w3.org/ns/hydra/core#property"), 
                    Node.createURI("http://www.w3.org/1999/02/22-rdf-syntax-ns#object"));
            boolean isTPF = graph.contains(triple0) && graph.contains(triple1) && graph.contains(triple2) &&
                    graph.contains(triple3) && graph.contains(triple4) && graph.contains(triple5) &&
                    graph.contains(triple6) && graph.contains(triple7) && graph.contains(triple8) && graph.contains(triple9);
            return isTPF;
        } catch (IOException ex) {            
        }
        return false;
    }
        
    private String getServerPath(Op op){
        String tmp, str = op.toString();
        tmp = str.substring(str.indexOf("service <")+"service <".length(), str.indexOf(">", str.lastIndexOf("service <")));
        return tmp;
    }
    
	private void init(DatasetGraph ds) {
		DatabaseConfig dbConfig = new DatabaseConfig();
		dbConfig.setAllowCreate(true);
		mainDB = context.env().openDatabase(null, "pri_cache_" + getId(), dbConfig);
		//System.out.println(op);
		//materialize the query result here;
		QueryIterator itr = context.loadGraphPattern(op, ds);
                if (op.toString().contains("service") && isTPFServer(getServerPath(op))) {
                    String s = op.toString();
                    int start = s.indexOf("(service");
                    int end = s.indexOf('(', s.indexOf("service"));
                    String str = s.substring(0, start) + s.substring(end, s.lastIndexOf(')')) + "\n";
                    Op oop = SSE.parseOp(str);
                    LinkedDataFragmentGraph ldfg = new LinkedDataFragmentGraph(getServerPath(op));
                    Model model = ModelFactory.createModelForGraph(ldfg);
                    itr = context.loadGraphPattern(oop, model);
                }
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
