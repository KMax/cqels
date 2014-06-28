package org.deri.cqels;

import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.graph.Triple;
import com.hp.hpl.jena.sparql.core.Var;
import static com.jayway.awaitility.Awaitility.*;
import java.io.File;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.Callable;
import org.deri.cqels.data.Mapping;
import org.deri.cqels.engine.ConstructListener;
import org.deri.cqels.engine.ContinuousConstruct;
import org.deri.cqels.engine.ContinuousListener;
import org.deri.cqels.engine.ContinuousSelect;
import org.deri.cqels.engine.ExecContext;
import org.deri.cqels.engine.RDFStream;
import static org.hamcrest.Matchers.*;
import static org.junit.Assert.*;
import org.junit.BeforeClass;
import org.junit.Test;

public class QueryTest {

    private static final String STREAM_ID_PREFIX = "http://example.org/simpletest/test";
    private static final String CQELS_HOME = "cqels_home";
    private static ExecContext context;

    @BeforeClass
    public static void beforeClass() {
        File home = new File(CQELS_HOME);
        if (!home.exists()) {
            home.mkdir();
        }
        context = new ExecContext(CQELS_HOME, true);
    }

    @Test(timeout = 5000)
    public void simpleSelect() {
        final String STREAM_ID = STREAM_ID_PREFIX + "_1";
        RDFStream stream = new DefaultRDFStream(context, STREAM_ID);

        ContinuousSelect query = context.registerSelect(""
                + "SELECT ?x ?y ?z WHERE {"
                + "STREAM <" + STREAM_ID + "> [NOW] {?x ?y ?z}"
                + "}");
        SelectAssertListener listener = new SelectAssertListener();
        query.register(listener);

        stream.stream(new Triple(
                Node.createURI("http://example.org/resource/1"),
                Node.createURI("http://example.org/ontology#hasValue"),
                Node.createLiteral("123")));

        List<Mapping> mappings = await().until(listener, hasSize(1));
        List<Node> nodes = toNodeList(mappings.get(0));
        assertEquals("http://example.org/resource/1", nodes.get(0).getURI());
        assertEquals("http://example.org/ontology#hasValue",
                nodes.get(1).getURI());
        assertEquals("123", nodes.get(2).getLiteralValue());
    }

    @Test(timeout = 5000)
    public void streamURIAsVar() {
        final String STREAM_ID = STREAM_ID_PREFIX + "_1";
        RDFStream stream = new DefaultRDFStream(context, STREAM_ID);

        context.loadDefaultDataset(
                "src/test/resources/org/deri/cqels/dataset.ttl");

        ContinuousSelect query = context.registerSelect(""
                + "SELECT ?x ?y ?z WHERE {"
                + "STREAM ?stream [NOW] {?x ?y ?z}"
                + "<http://example.org/resource/1> <http://example.org/ontology#hasStream> ?stream ."
                + "}");
        SelectAssertListener listener = new SelectAssertListener();
        query.register(listener);

        stream.stream(new Triple(
                Node.createURI("http://example.org/resource/1"),
                Node.createURI("http://example.org/ontology#hasValue"),
                Node.createLiteral("123")));

        List<Mapping> mappings = await().until(listener, hasSize(1));
        List<Node> nodes = toNodeList(mappings.get(0));
        assertEquals(3, nodes.size());
        assertEquals("http://example.org/resource/1", nodes.get(0).getURI());
        assertEquals("http://example.org/ontology#hasValue",
                nodes.get(1).getURI());
        assertEquals("123", nodes.get(2).getLiteralValue());
    }

    @Test
    public void severalStreamsAsVarsFromDataset() throws InterruptedException {
        RDFStream stream_1 = new DefaultRDFStream(context,
                STREAM_ID_PREFIX + "_1");
        RDFStream stream_2 = new DefaultRDFStream(context,
                STREAM_ID_PREFIX + "_2");

        context.loadDefaultDataset(
                "src/test/resources/org/deri/cqels/dataset.ttl");

        ContinuousSelect query = context.registerSelect(""
                + "SELECT ?x ?y ?z WHERE {"
                + "STREAM ?stream [NOW] {?x ?y ?z}"
                + "[] <http://example.org/ontology#hasStream> ?stream ."
                + "}");
        SelectAssertListener listener = new SelectAssertListener();
        query.register(listener);

        stream_1.stream(new Triple(
                Node.createURI("http://example.org/resource/1"),
                Node.createURI("http://example.org/ontology#hasValue"),
                Node.createLiteral("123")));
        stream_2.stream(new Triple(
                Node.createURI("http://example.org/resource/2"),
                Node.createURI("http://example.org/ontology#hasValue"),
                Node.createLiteral("321")));

        List<Mapping> mappings = await().until(listener, hasSize(2));

        List<Node> nodes = toNodeList(mappings.get(0));
        assertEquals("http://example.org/resource/1", nodes.get(0).getURI());
        assertEquals("http://example.org/ontology#hasValue",
                nodes.get(1).getURI());
        assertEquals("123", nodes.get(2).getLiteralValue());

        nodes = toNodeList(mappings.get(1));
        assertEquals("http://example.org/resource/2", nodes.get(0).getURI());
        assertEquals("http://example.org/ontology#hasValue",
                nodes.get(1).getURI());
        assertEquals("321", nodes.get(2).getLiteralValue());
    }

    @Test(timeout = 10000)
    public void simpleConstruct() {
        final String STREAM_ID = STREAM_ID_PREFIX + "_1";
        RDFStream stream = new DefaultRDFStream(context, STREAM_ID);

        ContinuousConstruct query = context.registerConstruct(""
                + "CONSTRUCT{?x ?y ?z} WHERE {"
                + "STREAM <" + STREAM_ID + "> [NOW] {?x ?y ?z}"
                + "}");
        ConstructAssertListener listener = new ConstructAssertListener(
                context, STREAM_ID);
        query.register(listener);

        stream.stream(new Triple(
                Node.createURI("http://example.org/resource/1"),
                Node.createURI("http://example.org/ontology#hasValue"),
                Node.createLiteral("123")));

        List<Triple> graph = await().until(listener, hasSize(1));
        assertEquals("http://example.org/resource/1", 
                graph.get(0).getSubject().getURI());
        assertEquals("http://example.org/ontology#hasValue",
                graph.get(0).getPredicate().getURI());
        assertEquals("123", graph.get(0).getObject().getLiteralLexicalForm());
    }

    private List<Node> toNodeList(Mapping mapping) {
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

    private class SelectAssertListener
            implements ContinuousListener,
            Callable<List<Mapping>> {

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

    private class ConstructAssertListener extends ConstructListener
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

    private class DefaultRDFStream extends RDFStream {

        public DefaultRDFStream(ExecContext context, String uri) {
            super(context, uri);
        }

        @Override
        public void stop() {
        }

    }

}
