package org.deri.cqels.helpers;

import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.graph.Triple;
import com.hp.hpl.jena.sparql.core.Var;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import org.deri.cqels.data.Mapping;
import org.deri.cqels.engine.ExecContext;

public class Helpers {
    
    public static List<Node> toNodeList(ExecContext context, Mapping mapping) {
        List<Node> nodes = new ArrayList<Node>();
        for (Iterator<Var> vars = mapping.vars(); vars.hasNext();) {
            final long id = mapping.get(vars.next());
            if (id > 0) {
                nodes.add(context.engine().decode(id));
            } else {
                nodes.add(null);
            }
        }
        return nodes;
    }
    
    public static void print(ExecContext context, List<Mapping> mappings) {
        for(Mapping m : mappings) {
            System.out.println(toNodeList(context, m));
        }
    }
    public static void print(List<Triple> graph){
        for(Triple t : graph){
            System.out.println(t);
        }
    }
    
}
