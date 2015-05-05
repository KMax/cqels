package org.deri.cqels.helpers;

import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.StmtIterator;
import org.deri.cqels.engine.ExecContext;
import org.deri.cqels.engine.RDFStream;

public class DefaultRDFStream extends RDFStream {

    public DefaultRDFStream(ExecContext context, String uri) {
        super(context, uri);
    }

    public void stream(Model t) {
        StmtIterator iter = t.listStatements();
        while (iter.hasNext()) {
            super.stream(iter.next().asTriple());
        }
    }

    @Override
    public void stop() {
    }
}
