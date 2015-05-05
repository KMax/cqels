package org.deri.cqels.helpers;

import com.hp.hpl.jena.graph.Triple;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Callable;
import org.deri.cqels.data.Mapping;
import org.deri.cqels.engine.ConstructListener;
import org.deri.cqels.engine.ContinuousListener;
import org.deri.cqels.engine.ExecContext;

public class AssertListeners {

    public static class SelectAssertListener
            implements ContinuousListener, Callable<List<Mapping>> {

        private final List<Mapping> mapping = Collections.synchronizedList(
                new ArrayList<Mapping>());

        @Override
        public void update(Mapping mapping) {
            this.mapping.add(mapping);
        }

        @Override
        public List<Mapping> call() throws Exception {
            return mapping;
        }

    }

    public static class ConstructAssertListener extends ConstructListener
            implements Callable<List<Triple>> {

        private final List<Triple> graph = Collections.synchronizedList(
                new ArrayList<Triple>());

        public ConstructAssertListener(ExecContext context, String streamUri) {
            super(context, streamUri);
        }

        @Override
        public void update(List<Triple> graph) {
            this.graph.addAll(graph);
        }

        @Override
        public List<Triple> call() throws Exception {
            return graph;
        }
    }
}
