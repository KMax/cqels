package org.deri.cqels;

import com.hp.hpl.jena.datatypes.xsd.XSDDatatype;
import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.graph.Triple;
import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.ModelFactory;
import com.hp.hpl.jena.rdf.model.ResourceFactory;
import static com.jayway.awaitility.Awaitility.await;
import com.jayway.awaitility.Duration;
import java.io.File;
import java.io.InputStream;
import java.util.List;
import org.deri.cqels.data.Mapping;
import org.deri.cqels.engine.ContinuousConstruct;
import org.deri.cqels.engine.ContinuousSelect;
import org.deri.cqels.engine.ExecContext;
import org.deri.cqels.engine.RDFStream;
import org.deri.cqels.helpers.AssertListeners.ConstructAssertListener;
import org.deri.cqels.helpers.AssertListeners.SelectAssertListener;
import org.deri.cqels.helpers.DefaultRDFStream;
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
        System.out.println("simpleUnregisterSelectTest");
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
    public void unregisterRemoteQuery() throws InterruptedException {
        System.out.println("unregisterRemoteQueryTest");
        System.out.println("remoteTPFTesttoDbpedia");
        final String rmtService = "http://fragments.dbpedia.org/2014/en";
        RDFStream stream = new DefaultRDFStream(context, STREAM_ID_PREFIX);

        ContinuousSelect query = context.registerSelect(""
                + "PREFIX rdf:<http://www.w3.org/1999/02/22-rdf-syntax-ns#>"
                + "PREFIX dbpedia-owl: <http://dbpedia.org/ontology/>"
                + "SELECT ?x ?y ?z ?p WHERE {"
                + "STREAM <" + STREAM_ID_PREFIX + "> [NOW] {?x ?y ?z}"
                + "SERVICE <" + rmtService + "> { <http://dbpedia.org/resource/11_Birthdays> ?x ?p.} "
                + "}");

        SelectAssertListener listener = new SelectAssertListener();
        query.register(listener);

        stream.stream(new Triple(
                Node.createURI("http://dbpedia.org/ontology/author"),
                Node.createURI("http://example.org/ontology#hasValue"),
                Node.createLiteral("1")));
        List<Mapping> mappings = await().until(listener, hasSize(1));
        List<Node> nodes = Helpers.toNodeList(context, mappings.get(0));

        Helpers.print(context, mappings);
        assertEquals("http://dbpedia.org/ontology/author", nodes.get(0).getURI());
        assertEquals("http://example.org/ontology#hasValue",
                nodes.get(1).getURI());
        assertEquals("1", nodes.get(2).getLiteralValue());
        assertEquals("http://dbpedia.org/resource/Wendy_Mass", nodes.get(3).getURI());

        context.unregisterSelect(query);

        stream.stream(new Triple(
                ResourceFactory.createResource("http://dbpedia.org/ontology/author").asNode(),
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
        System.out.println("simpleUnregisterConstructTest");
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
        assertEquals("456", triples.get(0).getObject().getLiteralLexicalForm());

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
        assertTrue(triples.size() == 1);
    }

    @Test
    public void complexUnregisterConstruct() {
        System.out.println("complexUnregisterConstructTest");
        final String STREAM_ID
                = "amqp://192.168.134.114?exchangeName=meter_exchange&routingKey=meter.location.mercury230_14759537";
        DefaultRDFStream stream = new DefaultRDFStream(context, STREAM_ID);

        context.loadDefaultDataset(
                "src/test/resources/org/deri/cqels/QueryTest/complexConstruct/dataset.ttl");

        ContinuousConstruct query = context.registerConstruct(
                "PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> \n"
                + "PREFIX em: <http://purl.org/NET/ssnext/electricmeters#> \n"
                + "PREFIX pne: <http://data.press.net/ontology/event/> \n"
                + "PREFIX ssn: <http://purl.oclc.org/NET/ssnx/ssn#> \n"
                + "PREFIX xsd: <http://www.w3.org/2001/XMLSchema#> \n"
                + "PREFIX dul: <http://www.loa-cnr.it/ontologies/DUL.owl#> \n"
                + "CONSTRUCT {\n"
                + "	?alert a <http://purl.org/daafse/alerts#TooHighVoltageValue> ;\n"
                + "		dul:hasEventDate ?time ;\n"
                + "		dul:involvesAgent ?meter .\n"
                + "}\n"
                + "WHERE {\n"
                + "  ?meter em:hasStream ?stream .\n"
                + "  STREAM ?stream [NOW] {\n"
                + "    ?observation a em:PolyphaseVoltageObservation ;\n"
                + "              ssn:observationResultTime ?time ;\n"
                + "              ssn:observationResult ?output ."
                + "    ?output a em:PolyphaseVoltageSensorOutput ;\n"
                + "              ssn:isProducedBy ?meter ;\n"
                + "              ssn:hasValue ?value .\n"
                + "    ?value em:hasQuantityValue ?qvalue .\n"
                + "  }\n"
                + "  FILTER(?qvalue > 220)\n"
                + "  BIND(IRI(CONCAT(STR(?meter), '/alerts/', STR(?time))) AS ?alert)\n"
                + "}");
        ConstructAssertListener listener = new ConstructAssertListener(
                context, STREAM_ID);
        query.register(listener);

        Model model = ModelFactory.createDefaultModel();
        InputStream input = this.getClass().getResourceAsStream(
                "QueryTest/complexConstruct/observation.ttl");
        model.read(input, null, "TURTLE");

        stream.stream(model);

        List<Triple> graph = await().until(listener, hasSize(3));
        
        assertTrue(graph.contains(new Triple(
                Node.createURI("http://purl.org/meters/mercury230_14759537/alerts/2014-07-16T05:50:20.890Z"),
                Node.createURI("http://www.w3.org/1999/02/22-rdf-syntax-ns#type"),
                Node.createURI("http://purl.org/daafse/alerts#TooHighVoltageValue"))));
        assertTrue(graph.contains(new Triple(
                Node.createURI("http://purl.org/meters/mercury230_14759537/alerts/2014-07-16T05:50:20.890Z"),
                Node.createURI("http://www.loa-cnr.it/ontologies/DUL.owl#hasEventDate"),
                Node.createLiteral("2014-07-16T05:50:20.890Z", XSDDatatype.XSDdateTime))));
        assertTrue(graph.contains(new Triple(
                Node.createURI("http://purl.org/meters/mercury230_14759537/alerts/2014-07-16T05:50:20.890Z"),
                Node.createURI("http://www.loa-cnr.it/ontologies/DUL.owl#involvesAgent"),
                Node.createURI("http://purl.org/meters/mercury230_14759537"))));
        
        context.unregisterConstruct(query);
        
        stream.stream(model);
        graph = await().timeout(Duration.TWO_HUNDRED_MILLISECONDS).until(listener, hasSize(3));
        assertTrue(graph.size()==3);
    }
}
