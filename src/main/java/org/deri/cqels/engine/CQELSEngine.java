package org.deri.cqels.engine;

import org.deri.cqels.data.EnQuad;

import com.espertech.esper.client.EPServiceProvider;
import com.espertech.esper.client.EPServiceProviderManager;
import com.espertech.esper.client.EPStatement;

import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.graph.Triple;
import com.hp.hpl.jena.sparql.core.Quad;
import com.hp.hpl.jena.tdb.store.NodeId;
/** 
 * This class implements CQELS engine. It has an Esper Service provider and context belonging to
 * 
 * @author		Danh Le Phuoc
 * @author 		Chan Le Van
 * @organization DERI Galway, NUIG, Ireland  www.deri.ie
 * @email 	danh.lephuoc@deri.org
 * @email   chan.levan@deri.org
 */
public class CQELSEngine {
	EPServiceProvider provider;
	ExecContext context;
    
	/**
	 * @param context the context this engine belonging to
	 */
	public CQELSEngine(ExecContext context) {
		this.provider = EPServiceProviderManager.getDefaultProvider();
		this.context = context;
	}
	
	/**
	 * send a quad to to engine
	 * @param quad the quad will be sent after encoded
	 */
	public void send(Quad quad) {
		this.provider.getEPRuntime().sendEvent(encode(quad));
	}
	
	/**
	 * send a quad which is represented as a graph node and a triple to engine
	 * @param graph graph node
	 * @param triple
	 */
	public void send(Node graph, Triple triple) {
		this.provider.getEPRuntime().sendEvent(encode(graph,triple));
	}
	
	/**
	 * send a quad which is represented as a graph, subject, 
	 * predicate and object node to engine
	 * @param graph graph node
	 * @param s subject
	 * @param p predicate
	 * @param o object
	 */
	public void send(Node graph, Node s, Node p, Node o) {
		this.provider.getEPRuntime().sendEvent(new EnQuad(encode(graph), encode(s), 
														  encode(p), encode(o)));
	}
	
	public EPStatement addWindow(Quad quad, String window) {
		return 	this.provider.getEPAdministrator().createEPL(
					"select * from org.deri.cqels.data.EnQuad" 
				    + matchPattern(quad) + window);
	}
	
	public EPStatement addSmt(String stmt) {
		return 	this.provider.getEPAdministrator().createEPL(stmt);
	}
	
	private String matchPattern(Quad quad) {
		String st = "", and = ""; 
		
		if(!quad.getGraph().isVariable()) { 
			st += "GID=" + context.dictionary().getAllocateNodeId(quad.getGraph()).getId();
			and = ", ";
		}
		
		if(!quad.getSubject().isVariable()) { 
			st += and + "SID=" + context.dictionary()
					 			.getAllocateNodeId(quad.getSubject()).getId();
			and = ", ";
		}
		
		if(!quad.getPredicate().isVariable()) { 
			st += and + "PID=" + context.dictionary()
								.getAllocateNodeId(quad.getPredicate()).getId();
			and = ", ";
		}
		
		if(!quad.getObject().isVariable()) { 
			st += and + "OID=" + context.dictionary().
								 getAllocateNodeId(quad.getObject()).getId();
			and = ", ";
		}
		
		return (st == "") ? "" : "(" + st + ")";
	}
       
	private EnQuad encode(Quad quad) {
		return new EnQuad(encode(quad.getGraph()), encode(quad.getSubject()), 
					      encode(quad.getPredicate()), encode(quad.getObject()));
	}
	
	private EnQuad encode(Node graph,Triple triple) {
		return new EnQuad(encode(graph), encode(triple.getSubject()), 
				          encode(triple.getPredicate()), encode(triple.getObject()));
	}
	
	public long encode(Node node) { 
		return this.context.dictionary().getAllocateNodeId(node).getId();
	}
	
	public Node decode(Long id) {
		return this.context.dictionary().getNodeForNodeId(NodeId.create(id));
	}
		
	public boolean match(Node n, long id) {		
		if (n.isVariable()) 
			return true; 
		return id == context.dictionary().getAllocateNodeId(n).getId();
	}
	
	public boolean match(EnQuad quad,Triple t) {
		return match(t.getSubject(),quad.getSID()) 
			 & match(t.getPredicate(),quad.getPID())
			 & match(t.getObject(),quad.getOID());
	}
}
