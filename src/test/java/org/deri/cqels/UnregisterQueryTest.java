package org.deri.cqels;

import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.graph.Triple;
import com.hp.hpl.jena.rdf.model.ResourceFactory;
import static com.jayway.awaitility.Awaitility.await;
import com.jayway.awaitility.Duration;
import java.io.File;
import java.util.List;
import org.deri.cqels.data.Mapping;
import org.deri.cqels.engine.ContinuousSelect;
import org.deri.cqels.engine.ExecContext;
import org.deri.cqels.engine.RDFStream;
import org.deri.cqels.helpers.AssertListeners.SelectAssertListener;
import org.deri.cqels.helpers.DefaultRDFStream;
import static org.hamcrest.Matchers.hasSize;
import static org.deri.cqels.helpers.AssertListeners.*;
import org.deri.cqels.helpers.Helpers;
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
        
        context.unregisterSelect(query);

        stream.stream(new Triple(
                ResourceFactory.createResource("http://example.org/resource/1").asNode(),
                ResourceFactory.createProperty("http://example.org/ontology#hasValue").asNode(),
                ResourceFactory.createPlainLiteral("123").asNode()));

        mappings = await()
                .timeout(Duration.TWO_HUNDRED_MILLISECONDS)
                .until(listener, hasSize(1));
        Helpers.print(context, mappings);
    }
}
