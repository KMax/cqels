package org.deri.cqels.engine;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.deri.cqels.data.Mapping;
import org.deri.cqels.lang.cqels.ElementStreamGraph;
import org.deri.cqels.lang.cqels.OpStream;
import org.openjena.atlas.lib.SetUtils;

import com.espertech.esper.client.EPStatement;
import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.graph.Triple;
import com.hp.hpl.jena.query.Query;
import com.hp.hpl.jena.sparql.algebra.Op;
import com.hp.hpl.jena.sparql.algebra.OpVars;
import com.hp.hpl.jena.sparql.algebra.op.OpDistinct;
import com.hp.hpl.jena.sparql.algebra.op.OpFilter;
import com.hp.hpl.jena.sparql.algebra.op.OpJoin;
import com.hp.hpl.jena.sparql.algebra.op.OpProject;
import com.hp.hpl.jena.sparql.core.Quad;
import com.hp.hpl.jena.sparql.core.TriplePath;
import com.hp.hpl.jena.sparql.core.Var;
import com.hp.hpl.jena.sparql.expr.Expr;
import com.hp.hpl.jena.sparql.expr.ExprAggregator;
import com.hp.hpl.jena.sparql.syntax.Element;
import com.hp.hpl.jena.sparql.syntax.ElementFilter;
import com.hp.hpl.jena.sparql.syntax.ElementGroup;
import com.hp.hpl.jena.sparql.syntax.ElementPathBlock;
import com.hp.hpl.jena.sparql.syntax.ElementTriplesBlock;
import com.hp.hpl.jena.sparql.syntax.Template;
/** 
 * This class uses heuristic approach to build an execution plan
 * 
 * @author		Danh Le Phuoc
 * @author 		Chan Le Van
 * @organization DERI Galway, NUIG, Ireland  www.deri.ie
 * @email 	danh.lephuoc@deri.org
 * @email   chan.levan@deri.org
 */
public class HeuristicRoutingPolicy extends RoutingPolicyBase {
    
    public HeuristicRoutingPolicy(ExecContext context) {    
    	super(context);
    	this.compiler = new LogicCompiler();
    	this.compiler.set(this);
    }
    
    /**
     * Creating the policy to route the mapping data
     * @param query
     * @return a router representing a tree of operators
     */   
    @Override
    public OpRouter generateRoutingPolicy(Query query) {
    	ElementGroup group = (ElementGroup)query.getQueryPattern();
    	
    	/* devide operators into three groups */
    	ArrayList<ElementFilter> filters = new ArrayList<ElementFilter>();
    	ArrayList<OpStream> streamOps = new ArrayList<OpStream>();
    	ArrayList<Op> others = new ArrayList<Op>();
    	for(Element el : group.getElements()) {
    		if(el instanceof ElementFilter) {
    			filters.add((ElementFilter)el);
    			continue;
    		}
    		if(el instanceof ElementStreamGraph) {
    			addStreamOp(streamOps, (ElementStreamGraph)el);
    			continue;
    		}
    		others.add(compiler.compile(el));
    	}
    	
    	/* push the filter down to operators on RDF datasets */
    	for(int i = 0; i < others.size(); i++) {
    		Op op = others.get(i);
    		for(ElementFilter filter : filters) {
    			if(OpVars.allVars(op).containsAll(filter.getExpr().getVarsMentioned())) {
    				op = OpFilter.filter(filter.getExpr(), op);
    			}
    		}
    		others.set(i, op);
    	}
    	
    	/*project the necessary variables */
    	project(filters, streamOps, others, query);
    	
    	/* Initialize query execution context, download named graph, create cache?.... */
    	for(String uri : query.getNamedGraphURIs()) {
    		if(!context.getDataset().containsGraph(Node.createURI(uri))) {
    			System.out.println(" load" +uri);
    			context.loadDataset(uri, uri);
    		}
    	}
    	    	
    	/* create Leaf cache from the operator over RDF datasets */
    	ArrayList<BDBGraphPatternRouter> caches = new ArrayList<BDBGraphPatternRouter>();
    	for(Op op : others) {
    		caches.add(new BDBGraphPatternRouter(context, op));
    	}
    	
    	/* create router for window operators */
    	ArrayList<IndexedTripleRouter> windows = new ArrayList<IndexedTripleRouter>();
    	for(OpStream op : streamOps) {
    		IndexedTripleRouter router = new IndexedTripleRouter(context, op);
    		Quad quad = new Quad(op.getGraphNode(), op.getBasicPattern().get(0));
    		EPStatement stmt = context.engine().addWindow(quad, ".win:length(1)");
    		stmt.setSubscriber(router);
    		windows.add(router);
    	}
    	/*
    	 * create routing plan for each window operator
    	 * how to route ???? hash operator is not unique
    	 */
    	int i = 0;
    	ArrayList<OpRouter> dataflows = new ArrayList<OpRouter>();
    	for(IndexedTripleRouter router : windows) {
    		BitSet wFlag = new BitSet(windows.size());
    		wFlag.set(i++);
    		BitSet cFlag = new BitSet(others.size());
    		BitSet fFlag = new BitSet(filters.size());
    		
    		Op curOp = router.getOp();
    		OpRouter curRouter = router;
    		Set<Var> curVars = (Set<Var>)OpVars.allVars(curOp);
    		int count = 1;
    		int curCount = 1;
    		
    		while(wFlag.size() + cFlag.size() + fFlag.size() > count) {
    			curCount = count;
    			boolean skip = false;
    			for(int j = 0; j < filters.size(); j++) {
    				if((!fFlag.get(j)) && curVars.containsAll(
    							filters.get(j).getExpr().getVarsMentioned())) {
    					curOp = OpFilter.filter(filters.get(j).getExpr(), curOp);
    					OpRouter newRouter = new FilterExprRouter(context,(OpFilter)curOp,curRouter);
    					curRouter = addRouter(curRouter, newRouter);
    					fFlag.set(j);
    					count++;
    				}
    			}
    			
    			for(int j = 0; j < caches.size() && (!skip); j++) {
    				if((!cFlag.get(j)) && SetUtils.intersectionP(curVars, 
    							(Set<Var>)OpVars.allVars(caches.get(j).getOp()))) {
    					curOp = OpJoin.create(curOp,caches.get(j).getOp());
    					OpRouter newRouter = new JoinRouter(context, (OpJoin)curOp, 
    														curRouter, caches.get(j));
    					curRouter = addRouter(curRouter, newRouter);
    					cFlag.set(j);
    					curVars.addAll(OpVars.allVars(caches.get(j).getOp()));
    					count++;
    					skip = true;
    				}
    			}
    			
    			for(int j = 0; j < windows.size() && (!skip); j++) {
    				if((!wFlag.get(j)) && SetUtils.intersectionP(curVars, 
    							(Set<Var>)OpVars.allVars(windows.get(j).getOp()))) {
    					curOp = OpJoin.create(curOp,windows.get(j).getOp());
    					OpRouter newRouter = new JoinRouter(context, (OpJoin)curOp, 
    														curRouter,windows.get(j));
    					curRouter = addRouter(curRouter, newRouter);
    					wFlag.set(j);
    					curVars.addAll(OpVars.allVars(windows.get(j).getOp()));
    					count++;
    					skip = true;
    				}
    			}
    			if(curCount == count) {
    				break;
    			}
    		}
    		dataflows.add(curRouter);
    	}
    	
    	ThroughRouter througRouter = new ThroughRouter(context, dataflows);
    	for(OpRouter r : dataflows) {
    		next.put(r.getId(), througRouter);
    	}
    	    	
    	//put all into routing policy
    	//return compileModifiers(query, througRouter);
    	return compiler.compileModifiers(query, througRouter);
    }
    
    /**
     * create the relationship between 2 router nodes. This version uses hash table to identify
     * which router will be the next for the mapping to go
     * @param from the departing router
     * @param newRouter the arriving router
     * @return the arriving router
     */
    @Override
    public OpRouter addRouter(OpRouter from, OpRouter newRouter) {
    	next.put(from.getId(), newRouter);
    	return newRouter;
    }
    
    /**
     * get the next router from the current router
     * @param curRouter current router
     * @param mapping
     * @return the next router
     */
	public OpRouter next(OpRouter curRouter, Mapping mapping) {
		//System.out.println("next"+next.get(curRouter.getId()));
		return next.get(curRouter.getId());
	}
    
	/**
	 * register the select-type query with the engine
	 * @param query 
	 * @return 
	 */
    public ContinuousSelect registerSelectQuery(Query query) {
    	OpRouter qR = generateRoutingPolicy(query);
    	if(query.isSelectType()) {
    		///TODO
    		ContinuousSelect rootRouter =(ContinuousSelect)addRouter(qR, 
    				new ContinuousSelect(context, query, qR));
    		rootRouter.visit(new TimerVisitor());
    		return rootRouter;
    	}
    	return null;
    }
     
	/**
	 * register the construct-type query with the engine
	 * @param query 
	 * @return 
	 */
    public ContinuousConstruct registerConstructQuery(Query query) {
    	OpRouter qR = generateRoutingPolicy(query);
    	if(query.isConstructType()) {
    		///TODO
    		ContinuousConstruct rootRouter = (ContinuousConstruct)addRouter(qR, 
    				new ContinuousConstruct(context, query, qR)); 
     		return rootRouter;
     	}
    	return null;
    }
    
    /**
	 * @param filters
	 * @param streamOps
	 * @param others
	 */
	private void project(ArrayList<ElementFilter> filters,
			ArrayList<OpStream> streamOps, ArrayList<Op> others,Query query) {
		
		HashSet<Var> upperVars = new HashSet<Var>();
		upperVars.addAll(query.getProjectVars());
		if (query.hasGroupBy()) {
            upperVars.addAll(query.getGroupBy().getVars());
            for(ExprAggregator agg:query.getAggregators())
            	upperVars.addAll(agg.getVarsMentioned());
        }
		
		if(query.hasHaving()) {
			if (query.hasHaving()) {
	            for (Expr expr : query.getHavingExprs()) {
	            	upperVars.addAll(expr.getVarsMentioned());
	            }
	        }
		}
		
    	for(ElementFilter filter : filters) {
    			upperVars.addAll(filter.getExpr().getVarsMentioned());
    	    	//System.out.println(upperVars);
    	}

    	for(OpStream op : streamOps) {
    		OpVars.allVars(op,upperVars);
    		//System.out.println(upperVars);
    	}
    	
    	for(int i = 0; i < others.size(); i++) {
    		Op op = others.get(i);
    		Set<Var> opVars = (Set<Var>)OpVars.allVars(op);
    		ArrayList<Var> projectedVars = new ArrayList<Var>();
    		for(Var var:opVars) {
    			if(upperVars.contains(var)) projectedVars.add(var);
    		}
    		if(projectedVars.size()<opVars.size()) {
    			others.set(i, new OpDistinct(new OpProject(op,projectedVars)));
    		}
    		else {
    			others.set(i, new OpDistinct(op));
    		}
    	}
	}
    
    private void addStreamOp(ArrayList<OpStream> streamOps, ElementStreamGraph el) {
    	/*if(el.getWindow()==null){
    		System.out.println("null");
    	}
    	else System.out.println(el.getWindow().getClass());*/
    	if(el.getElement() instanceof ElementTriplesBlock) {
    		addStreamOp(streamOps, (ElementTriplesBlock)el.getElement(), 
    				    el.getGraphNameNode(), el.getWindow());
    	}
    	else if(el.getElement() instanceof ElementGroup) {
    		addStreamOp(streamOps, (ElementGroup)el.getElement(), 
    				    el.getGraphNameNode(),el.getWindow());
    	}
    	else {
    		System.out.println("Stream pattern is not ElementTripleBlock" + el.getElement().getClass());
    	}
    }
    
    private void addStreamOp(ArrayList<OpStream> streamOps,
			ElementGroup group, Node graphNode,Window window) {
		for(Element el:group.getElements()) {
			if(el instanceof ElementTriplesBlock) {
				addStreamOp(streamOps, (ElementTriplesBlock)el, graphNode, window);
			}
			if(el instanceof ElementPathBlock) {
				for(Iterator<TriplePath> paths = ((ElementPathBlock)el).patternElts(); paths.hasNext(); ) {
					Triple t = paths.next().asTriple();
					if(t != null)
						streamOps.add(new OpStream(graphNode, t, window));
					else {
						System.out.println("Path is not supported");
					}
				}
			}
			else {
				System.out.println("unrecognized block" + el.getClass());
			}
		}
	}

	private void addStreamOp(ArrayList<OpStream> streamOps,
			ElementTriplesBlock el, Node graphNode,Window window) {
		for(Triple t:el.getPattern().getList()) {
			streamOps.add(new OpStream(graphNode,t,window));
		}
	}
}
