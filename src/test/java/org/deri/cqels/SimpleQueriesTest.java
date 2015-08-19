package org.deri.cqels;

import com.hp.hpl.jena.datatypes.xsd.XSDDatatype;
import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.graph.Triple;
import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.ModelFactory;
import com.hp.hpl.jena.rdf.model.Statement;
import static com.jayway.awaitility.Awaitility.*;
import java.io.File;
import java.io.InputStream;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Arrays;
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
import static org.hamcrest.Matchers.*;
import static org.junit.Assert.*;
import static org.deri.cqels.helpers.AssertListeners.*;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

public class SimpleQueriesTest {

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
        List<Node> nodes = Helpers.toNodeList(context, mappings.get(0));
        assertEquals("http://example.org/resource/1", nodes.get(0).getURI());
        assertEquals("http://example.org/ontology#hasValue",
                nodes.get(1).getURI());
        assertEquals("123", nodes.get(2).getLiteralValue());
    }
    
    @Test
    public void simpleRemoteTest() {
        System.out.println("simpleRemoteTest");
        final String rmtService = "http://rdf.myexperiment.org/sparql";
        RDFStream stream = new DefaultRDFStream(context, STREAM_ID_PREFIX);

        ContinuousSelect query = context.registerSelect(""
                + "PREFIX rdf:<http://www.w3.org/1999/02/22-rdf-syntax-ns#>"
                + "SELECT ?x ?y ?z ?p WHERE {"
                + "STREAM <" + STREAM_ID_PREFIX + "> [NOW] {?x ?y ?z}"
                + "SERVICE <" + rmtService + "> { <http://rdf.myexperiment.org/ontologies/snarm/Policy> ?x ?p} "
                + "}");        
        SelectAssertListener listener = new SelectAssertListener();
        query.register(listener);

        stream.stream(new Triple(
                Node.createURI("http://www.w3.org/2000/01/rdf-schema#isDefinedBy"),
                Node.createURI("http://example.org/ontology#hasValue"),
                Node.createLiteral("123")));
        List<Mapping> mappings = await().until(listener, hasSize(1));
        List<Node> nodes = Helpers.toNodeList(context, mappings.get(0));

        Helpers.print(context, mappings);
        assertEquals("http://www.w3.org/2000/01/rdf-schema#isDefinedBy", nodes.get(0).getURI());
        assertEquals("http://example.org/ontology#hasValue",
                nodes.get(1).getURI());
        assertEquals("123", nodes.get(2).getLiteralValue());
        assertEquals("http://rdf.myexperiment.org/ontologies/snarm/", nodes.get(3).getURI());
    }

    @Test
    //@Ignore
    public void simpleRemoteLDFTest() {
        System.out.println("simpleRemoteLDFTest");
        final String rmtService = "http://ldf.lodlaundromat.org/0c77296e83c678a8757669a19894bde6";
        RDFStream stream = new DefaultRDFStream(context, STREAM_ID_PREFIX);

        ContinuousSelect query = context.registerSelect(""
                + "PREFIX rdf:<http://www.w3.org/1999/02/22-rdf-syntax-ns#>"
                + "PREFIX dbpedia-owl: <http://dbpedia.org/ontology/>"
                + "SELECT ?x ?y ?z ?p WHERE {"
                + "STREAM <" + STREAM_ID_PREFIX + "> [NOW] {?x ?y ?z}"
                + "SERVICE <" + rmtService + "> { <http://river.styx.org/ww/2010/12/cablegraph#04VATICAN3196> ?x ?p.} "
                + "}");

        SelectAssertListener listener = new SelectAssertListener();
        query.register(listener);

        stream.stream(new Triple(
                Node.createURI("http://purl.org/dc/terms/accessRights"),
                Node.createURI("http://example.org/ontology#hasValue"),
                Node.createLiteral("123")));
        List<Mapping> mappings = await().until(listener, hasSize(1));
        List<Node> nodes = Helpers.toNodeList(context, mappings.get(0));

        Helpers.print(context, mappings);
        assertEquals("http://purl.org/dc/terms/accessRights", nodes.get(0).getURI());
        assertEquals("http://example.org/ontology#hasValue",
                nodes.get(1).getURI());
        assertEquals("123", nodes.get(2).getLiteralValue());
        assertEquals("http://river.styx.org/ww/2010/12/cablegraph#CONFIDENTIAL", nodes.get(3).getURI());
    }

    @Test(timeout = 5000)
    public void simpleSelectWithBindConcatStrStr() {
        final String STREAM_ID = STREAM_ID_PREFIX + "_1";
        RDFStream stream = new DefaultRDFStream(context, STREAM_ID);

        ContinuousSelect query = context.registerSelect(""
                + "SELECT ?x ?y ?xz WHERE {"
                + "  STREAM <" + STREAM_ID + "> [NOW] {"
                + "    ?x ?y ?z ."
                + "  }"
                + "  BIND(CONCAT(STR(?x), STR(?z)) AS ?xz)"
                + "}");
        SelectAssertListener listener = new SelectAssertListener();
        query.register(listener);

        stream.stream(new Triple(
                Node.createURI("http://example.org/resource/1"),
                Node.createURI("http://example.org/ontology#hasValue"),
                Node.createLiteral("123")));

        List<Mapping> mappings = await().until(listener, hasSize(1));
        List<Node> nodes = Helpers.toNodeList(context, mappings.get(0));
        System.out.println(Arrays.toString(nodes.toArray()));
        assertEquals("http://example.org/resource/1", nodes.get(0).getURI());
        assertEquals("http://example.org/ontology#hasValue",
                nodes.get(1).getURI());
        assertEquals("http://example.org/resource/1123", nodes.get(2).getLiteralValue());
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
        List<Node> nodes = Helpers.toNodeList(context, mappings.get(0));
        assertEquals(3, nodes.size());
        assertEquals("http://example.org/resource/1", nodes.get(0).getURI());
        assertEquals("http://example.org/ontology#hasValue",
                nodes.get(1).getURI());
        assertEquals("123", nodes.get(2).getLiteralValue());
    }

    @Test(timeout = 5000)
    public void severalStreamsAsVarsFromDataset() {
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

        List<Node> nodes = Helpers.toNodeList(context, mappings.get(0));
        assertEquals("http://example.org/resource/1", nodes.get(0).getURI());
        assertEquals("http://example.org/ontology#hasValue",
                nodes.get(1).getURI());
        assertEquals("123", nodes.get(2).getLiteralValue());

        nodes = Helpers.toNodeList(context, mappings.get(1));
        assertEquals("http://example.org/resource/2", nodes.get(0).getURI());
        assertEquals("http://example.org/ontology#hasValue",
                nodes.get(1).getURI());
        assertEquals("321", nodes.get(2).getLiteralValue());
    }

    @Test(timeout = 5000)
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

    @Test
    @Ignore //Manual test
    public void selectRemoteSPARQL() {
        final String STREAM_ID
                = "amqp://192.168.134.114?exchangeName=meter_exchange&routingKey=meter.location.mercury230_16824038";
        RDFStream stream = new DefaultRDFStream(context, STREAM_ID);

        ContinuousSelect query = context.registerSelect(""
                + "PREFIX em: <http://purl.org/NET/ssnext/electricmeters#>\n"
                + "SELECT ?x ?y ?z "
                + "FROM NAMED <http://192.168.134.114:8890/sparql-graph-crud>"
                + "WHERE {"
                + "STREAM ?stream [NOW] {?x ?y ?z}"
                + "GRAPH <http://192.168.134.114/SmartMetersDB/> {"
                + "<http://purl.org/daafse/meters/mercury230_16824038> em:hasStream ?stream}"
                + "}");
        SelectAssertListener listener = new SelectAssertListener();
        query.register(listener);

        stream.stream(new Triple(
                Node.createURI("http://example.org/resource/1"),
                Node.createURI("http://example.org/ontology#hasValue"),
                Node.createLiteral("123")));

        List<Mapping> mappings = await().until(listener, hasSize(1));
        List<Node> nodes = Helpers.toNodeList(context, mappings.get(0));
        assertEquals("http://example.org/resource/1", nodes.get(0).getURI());
        assertEquals("http://example.org/ontology#hasValue",
                nodes.get(1).getURI());
        assertEquals("123", nodes.get(2).getLiteralValue());
    }

    @Test(timeout = 10000)
    public void complexConstruct() {
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
        System.out.println(Arrays.toString(graph.toArray()));
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
    }

    @Test(timeout = 10000)
    public void selectAvg() {
        final String STREAM_ID = STREAM_ID_PREFIX + "_1";
        RDFStream stream = new DefaultRDFStream(context, STREAM_ID);

        ContinuousSelect query = context.registerSelect(""
                + "SELECT (AVG(?z) AS ?avg) WHERE {"
                + "STREAM <" + STREAM_ID + "> [NOW] {"
                + "?x ?y ?z"
                + "}"
                + "}"
                + "GROUP BY ?x");
        SelectAssertListener listener = new SelectAssertListener();
        query.register(listener);

        List<Triple> triples = readTriples("QueryTest/selectAvgStream.n3");

        for (Triple triple : triples) {
            stream.stream(triple);
        }

        List<Mapping> mappings = await().until(listener, hasSize(6));
        for (Mapping m : mappings) {
            List<Node> nodes = Helpers.toNodeList(context, m);
        }
        
        List<Node> r_0 = Helpers.toNodeList(context, mappings.get(0));
        assertEquals(3, r_0.get(0).getLiteralValue());
        List<Node> r_1 = Helpers.toNodeList(context, mappings.get(1));
        assertEquals(new BigDecimal(2.5), r_1.get(0).getLiteralValue());
        List<Node> r_2 = Helpers.toNodeList(context, mappings.get(2));
        assertEquals(2, r_2.get(0).getLiteralValue());
        List<Node> r_3 = Helpers.toNodeList(context, mappings.get(3));
        assertEquals(6, r_3.get(0).getLiteralValue());
        List<Node> r_4 = Helpers.toNodeList(context, mappings.get(4));
        assertEquals(new BigDecimal(5.5), r_4.get(0).getLiteralValue());
        List<Node> r_5 = Helpers.toNodeList(context, mappings.get(5));
        assertEquals(5, r_5.get(0).getLiteralValue());
    }

    private List<Triple> readTriples(final String path) {
        List<Triple> triples = new ArrayList<Triple>();

        Model model = ModelFactory.createDefaultModel();
        model.read(this.getClass().getResourceAsStream(path), null, "N-TRIPLE");

        List<Statement> statements = model.listStatements().toList();
        for (Statement s : statements) {
            triples.add(s.asTriple());
        }

        return triples;
    }

}
