package org.deri.cqels.engine;

import java.io.File;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import org.deri.cqels.lang.cqels.ParserCQELS;

import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.query.Query;
import com.hp.hpl.jena.sparql.algebra.Algebra;
import com.hp.hpl.jena.sparql.algebra.Op;
import com.hp.hpl.jena.sparql.core.DatasetGraph;
import com.hp.hpl.jena.sparql.engine.ExecutionContext;
import com.hp.hpl.jena.sparql.engine.QueryIterator;
import com.hp.hpl.jena.tdb.TDBFactory;
import com.hp.hpl.jena.tdb.base.file.FileFactory;
import com.hp.hpl.jena.tdb.base.file.FileSet;
import com.hp.hpl.jena.tdb.base.file.Location;
import com.hp.hpl.jena.tdb.index.IndexBuilder;
import com.hp.hpl.jena.tdb.nodetable.NodeTable;
import com.hp.hpl.jena.tdb.nodetable.NodeTableNative;
import com.hp.hpl.jena.tdb.solver.OpExecutorTDB;
import com.hp.hpl.jena.tdb.store.DatasetGraphTDB;
import com.hp.hpl.jena.tdb.store.bulkloader.BulkLoader;
import com.hp.hpl.jena.tdb.sys.SystemTDB;
import com.hp.hpl.jena.tdb.transaction.DatasetGraphTransaction;

import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentConfig;
/** 
 * This class implements CQELS execution context
 * 
 * @author		Danh Le Phuoc
 * @author 		Chan Le Van
 * @organization DERI Galway, NUIG, Ireland  www.deri.ie
 * @email 	danh.lephuoc@deri.org
 * @email   chan.levan@deri.org
 */
public class ExecContext {
	CQELSEngine engine;
	RoutingPolicy policy;
	Properties config;
	HashMap<String, Object> hashMap;
	HashMap<Integer,OpRouter> routers;
	DatasetGraphTDB dataset;
	NodeTable  dictionary;
	Location location;
	Environment env;
	ExecutionContext arqExCtx;
	
    /**
	 * @param path home path containing dataset
	 * @param cleanDataset a flag indicates whether the old dataset will be cleaned or not
	 */
	public ExecContext(String path, boolean cleanDataset) {
		this.hashMap = new HashMap<String, Object>();
		//combine cache and disk-based dictionary
		this.dictionary = new NodeTableNative(IndexBuilder.mem().newIndex(FileSet.mem(), 
											  		SystemTDB.nodeRecordFactory), 
											  FileFactory.createObjectFileMem(path));
		setEngine(new CQELSEngine(this));
		createCache(path + "/cache");
		if (cleanDataset) {
			cleanNCreate(path + "/datasets");
		}
		createDataSet(path + "/datasets");
		
		this.routers = new HashMap<Integer, OpRouter>();
		this.policy = new HeuristicRoutingPolicy(this);
	}
	
	static void cleanNCreate(String path) {
		deleteDir(new File(path));
		if(!(new File(path)).mkdir()) {
			System.out.println("can not create working directory"+path);
		}
	}
	
	/**
	 * to delete a directory
	 * @param dir directory will be deleted
	 */
	public static boolean deleteDir(File dir) {
        if (dir.isDirectory()) {
            String[] children = dir.list();
            for (int i=0; i<children.length; i++) {
                boolean success = deleteDir(new File(dir, children[i]));
                if (!success) {
                	System.out.println("can not delete" +dir);
                    return false;
                }
            }
        }
        return dir.delete();
	}
	/**
	 * get the ARQ context
	 */
	public ExecutionContext getARQExCtx() {
		return this.arqExCtx;
	}
	/**
	 * create a dataset with a specified location
	 * @param location the specified location string
	 */
	public void createDataSet(String location) {
		this.dataset = ((DatasetGraphTransaction)TDBFactory.createDatasetGraph(location)).getBaseDatasetGraph();
		this.arqExCtx = new ExecutionContext(this.dataset.getContext(), 
											this.dataset.getDefaultGraph(), 
											this.dataset, OpExecutorTDB.OpExecFactoryTDB);
	}
	
	/**
	 * load a dataset with the specified graph uri and data uri
	 * @param graphUri
	 * @param dataUri 
	 */
	public void loadDataset(String graphUri, String dataUri) {
		BulkLoader.loadNamedGraph(this.dataset, 
					Node.createURI(graphUri),Arrays.asList(dataUri) , false);
	}
	
	/**
	 * load a dataset with the specified data uri
	 * @param dataUri 
	 */
	public void loadDefaultDataset(String dataUri) {
		BulkLoader.loadDefaultGraph(this.dataset, Arrays.asList(dataUri) , true);
	}
	
	/**
	 * get the dataset
	 * @param dataUri 
	 */
	public DatasetGraphTDB getDataset() { 
		return dataset; 
	};
	
	/**
	 * create cache with the specified path
	 * @param cachePath path string 
	 */
	public void createCache(String cachePath) {
		cleanNCreate(cachePath);
        createEnv(cachePath);
	}
	
	private void createEnv(String path) {
	    EnvironmentConfig config = new EnvironmentConfig();
		config.setAllowCreate(true);
		this.env = new Environment(new File(path), config);
	}
	
	/**
	 * get environment
	 */
	public Environment env() { return this.env; }
	
	/**
	 * get CQELS engine
	 */
	public CQELSEngine engine() { return this.engine; }
	
	/**
	 * set CQELS engine
	 * @param engine
	 */
	public  void setEngine(CQELSEngine engine) { this.engine = engine; }
	
	/**
	 * get routing policy
	 */
	public RoutingPolicy policy() { return this.policy; }
	
	/**
	 * set routing policy with the specified policy
	 * @param policy specified policy and mostly heuristic policy in this version
	 */
	public void setPolicy(RoutingPolicy policy) { this.policy = policy; };
	
	/**
	 * put key and value to the map
	 * @param key
	 * @param value
	 */
	public void put(String key, Object value) { this.hashMap.put(key, value); }
	
	/**
	 * get the value with the specified key
	 * @param key 
	 */
	public Object get(String key) { return this.hashMap.get(key); }
	
	/**
	 * init TDB graph with the specified directory
	 * @param directory  
	 */
	public void initTDBGraph(String directory) { 
		//this.dataset = TDBFactory.createDatasetGraph(directory);
		this.dataset = ((DatasetGraphTransaction)TDBFactory.createDatasetGraph(location)).getBaseDatasetGraph();
	}
	
	/**
	 * load graph pattern
	 * @param op operator
	 * @return query iterator  
	 */
	public QueryIterator loadGraphPattern(Op op) { 
		return Algebra.exec(op, this.dataset); 
	}
	
	/**
	 * load graph pattern with the specified dataset
	 * @param op operator
	 * @param ds specified dataset
	 * @return query iterator  
	 */
	public QueryIterator loadGraphPattern(Op op, DatasetGraph ds) {
		return Algebra.exec(op, ds); 
	}
	
	/**
	 * get the cache location
	 * @return cache location
	 */
	public Location cacheLocation() { return this.location; }
	
	/**
	 * get the dictionary
	 * @return dictionary  
	 */
	public NodeTable dictionary() { return this.dictionary; }
	
	/**
	 * get cache configuration
	 * @return cache configuration  
	 */
	public Properties cacheConfig() { return  this.config; }
	
	/**
	 * @param idx 
	 * @return router   
	 */
	public void router(int idx, OpRouter router) { 
		this.routers.put(Integer.valueOf(idx), router);
	}
	
	/**
	 * @param idx 
	 * @return router   
	 */
	public OpRouter router(int idx) {
		return this.routers.get(Integer.valueOf(idx));
	}
	
	/**
	 * register a select query
	 * @param queryStr query string 
	 * @return this method return an instance of ContinuousSelect interface  
	 */
	public ContinuousSelect registerSelect(String queryStr) {
		 Query query = new Query();
	     ParserCQELS parser=new ParserCQELS();
	     parser.parse(query, queryStr);
	     return this.policy.registerSelectQuery(query);   
	}
	
	/**
	 * register a construct query
	 * @param queryStr query string 
	 * @return this method return an instance of ContinuousConstruct interface  
	 */
	public ContinuousConstruct registerConstruct(String queryStr) {
		 Query query = new Query();
	     ParserCQELS parser = new ParserCQELS();
	     parser.parse(query, queryStr);
	     return this.policy.registerConstructQuery(query);   
	}
}
