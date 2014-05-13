package org.deri.cqels.lang.cqels;

import org.deri.cqels.engine.Window;

import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.sparql.syntax.Element;
import com.hp.hpl.jena.sparql.syntax.ElementGroup;
import com.hp.hpl.jena.sparql.syntax.ElementNamedGraph;
import com.hp.hpl.jena.sparql.syntax.ElementVisitor;
import com.hp.hpl.jena.sparql.util.NodeIsomorphismMap;

public class ElementStreamGraph extends ElementNamedGraph {
	private Window window;
	public ElementStreamGraph(Node n, Window w, Element el) {
		super(n, el);
		window = w;
	}
	
	public Window getWindow() {	return window; }
}
