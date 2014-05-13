package org.deri.cqels.engine;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.deri.cqels.data.Mapping;

import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.graph.Triple;
import com.hp.hpl.jena.graph.TripleIterator;
import com.hp.hpl.jena.sparql.core.BasicPattern;
import com.hp.hpl.jena.sparql.syntax.Template;
/**
 * This class processes the mapping result for the CONSTRUCT-type query form
 * @author		Danh Le Phuoc
 * @author 		Chan Le Van
 * @organization DERI Galway, NUIG, Ireland  www.deri.ie
 * @email 	danh.lephuoc@deri.org
 * @email   chan.levan@deri.org
 * @see ContinuousListenerBase
 */
public abstract class ConstructListener implements ContinuousListener {
	String uri;
	Template template;
	ExecContext context;
	public static int count = 0;
	public ConstructListener(ExecContext context, String streamURI) { 
		this.uri = streamURI;
		this.context = context;
	}
	
	public ConstructListener(ExecContext context) {
		this.context = context;
	}
	public void setTemplate(Template t) {
		template = t;
	}

	
	public void update(Mapping mapping) {
		List<Triple> triples = template.getTriples();
		Iterator<Triple> ti = triples.iterator();
		ArrayList<Triple> graph = new ArrayList<Triple>();
		while(ti.hasNext()) {
			Triple triple = ti.next();
			Node s, p, o;
			
			if (triple.getSubject().isVariable()) {
				s = context.engine.decode(mapping.get(triple.getSubject()));
			}
			else {
				s = triple.getSubject();
			}
			
			if (triple.getPredicate().isVariable()) {
				p = context.engine.decode(mapping.get(triple.getPredicate()));
			}
			else {
				p = triple.getPredicate();
			}
			
			if (triple.getObject().isVariable()) {
				o = context.engine.decode(mapping.get(triple.getObject()));
			}
			else { 
				o = triple.getObject();
			}
			graph.add(new Triple(s, p, o));
		}
		update(graph);
		count ++;
	}
	
	public abstract void update(List<Triple> graph);

}
