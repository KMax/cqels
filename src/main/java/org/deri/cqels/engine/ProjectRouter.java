package org.deri.cqels.engine;

import java.util.List;

import org.deri.cqels.data.Mapping;
import org.deri.cqels.data.ProjectMapping;
import org.deri.cqels.engine.iterator.MappingIterator;
import org.deri.cqels.engine.iterator.NullMappingIter;

import com.hp.hpl.jena.sparql.algebra.op.OpProject;
import com.hp.hpl.jena.sparql.core.Var;

/**
 * This class implements the router with project operator
 *
 * @author	Danh Le Phuoc
 * @author Chan Le Van
 * @organization DERI Galway, NUIG, Ireland www.deri.ie
 * @email danh.lephuoc@deri.org
 * @email chan.levan@deri.org
 * @see OpRouter1
 */
public class ProjectRouter extends OpRouter1 {

    private final List<Var> vars;

    public ProjectRouter(ExecContext context, OpProject op, OpRouter sub) {
        super(context, op, sub);
        vars = op.getVars();
    }

    @Override
    public void route(Mapping mapping) {
        _route(new ProjectMapping(context, mapping, vars));
    }

    @Override
    public MappingIterator searchBuff4Match(Mapping mapping) {
        //TODO: check if necessary to call this method
        return NullMappingIter.instance();
    }

    @Override
    public MappingIterator getBuff() {
        return NullMappingIter.instance();
    }

    @Override
    public void visit(RouterVisitor rv) {
        rv.visit(this);
        sub().visit(rv);
    }
    
    public void destroy() {
        vars.clear();
        context.policy().removeRouter(sub(), this);
    }
}
