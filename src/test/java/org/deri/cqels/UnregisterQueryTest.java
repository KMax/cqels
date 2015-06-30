package org.deri.cqels;

import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.graph.Triple;
import com.hp.hpl.jena.rdf.model.ResourceFactory;
import static com.jayway.awaitility.Awaitility.await;
import com.jayway.awaitility.Duration;
import java.io.File;
import java.util.List;
import org.deri.cqels.data.Mapping;
import org.deri.cqels.engine.ContinuousConstruct;
import org.deri.cqels.engine.ContinuousSelect;
import org.deri.cqels.engine.ExecContext;
import org.deri.cqels.engine.RDFStream;
import org.deri.cqels.helpers.AssertListeners.SelectAssertListener;
import org.deri.cqels.helpers.DefaultRDFStream;
import static org.hamcrest.Matchers.hasSize;
import static org.deri.cqels.helpers.AssertListeners.*;
import org.deri.cqels.helpers.Helpers;
import static org.hamcrest.Matchers.hasSize;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import org.junit.BeforeClass;
import org.junit.Test;

public class UnregisterQueryTest {

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

    @Test
    public void test() throws InterruptedException {
        final String STREAM_ID = STREAM_ID_PREFIX + "_1";
        RDFStream stream = new DefaultRDFStream(context, STREAM_ID);

        ContinuousSelect query = context.registerSelect(""
                + "SELECT ?x ?y ?z WHERE {"
                + "STREAM <" + STREAM_ID + "> [NOW] {?x ?y ?z}"
                + "}");
        SelectAssertListener listener = new SelectAssertListener();
        query.register(listener);

        stream.stream(new Triple(
                ResourceFactory.createResource("http://example.org/resource/1").asNode(),
                ResourceFactory.createProperty("http://example.org/ontology#hasValue").asNode(),
                ResourceFactory.createPlainLiteral("123").asNode()));

        
        List<Mapping> mappings = await().until(listener, hasSize(1));
        Helpers.print(context, mappings);
        List<Node> nodes = Helpers.toNodeList(context, mappings.get(0));
        assertEquals("http://example.org/resource/1", nodes.get(0).getURI());
        assertEquals("http://example.org/ontology#hasValue",
                nodes.get(1).getURI());
        assertEquals("123", nodes.get(2).getLiteralValue());
        
        context.unregisterSelect(query);

        stream.stream(new Triple(
                ResourceFactory.createResource("http://example.org/resource/1").asNode(),
                ResourceFactory.createProperty("http://example.org/ontology#hasValue").asNode(),
                ResourceFactory.createPlainLiteral("789").asNode()));

        mappings = await()
                .timeout(Duration.TWO_HUNDRED_MILLISECONDS)
                .until(listener, hasSize(1));
        nodes = Helpers.toNodeList(context, mappings.get(0));
        assertTrue(nodes.isEmpty());
        Helpers.print(context, mappings);
    }
    
    @Test
    public void unregisterConstruct() throws InterruptedException {
        final String STREAM_ID = STREAM_ID_PREFIX + "_1";
        RDFStream stream = new DefaultRDFStream(context, STREAM_ID);

        ContinuousConstruct query = context.registerConstruct(""
                + "CONSTRUCT{?x ?y ?z} WHERE {"
                + "STREAM <" + STREAM_ID + "> [NOW] {?x ?y ?z}"
                + "}");
        ConstructAssertListener listener = new ConstructAssertListener(context,
                STREAM_ID);
        query.register(listener);

        stream.stream(new Triple(
                ResourceFactory.createResource(
                        "http://example.org/resource/1").asNode(),
                ResourceFactory.createProperty(
                        "http://example.org/ontology#hasValue").asNode(),
                ResourceFactory.createPlainLiteral("456").asNode()));

        List<Triple> triples = await().until(listener, hasSize(1));
        Helpers.print(triples);

        assertEquals("http://example.org/resource/1",
                triples.get(0).getSubject().getURI());
        assertEquals("http://example.org/ontology#hasValue",
                triples.get(0).getPredicate().getURI());
        assertEquals("123", triples.get(0).getObject().getLiteralLexicalForm());
        
        context.unregisterConstruct(query);

        stream.stream(new Triple(
                ResourceFactory.createResource(
                        "http://example.org/resource/1").asNode(),
                ResourceFactory.createProperty(
                        "http://example.org/ontology#hasValue").asNode(),
                ResourceFactory.createPlainLiteral("987789").asNode()));
        triples = await()
                .timeout(Duration.TWO_HUNDRED_MILLISECONDS)
                .until(listener, hasSize(1));
        Helpers.print(triples);
        assertTrue(triples.isEmpty());
    }
}
