package org.deri.cqels.engine;

import org.deri.cqels.data.ExtendMapping;
import org.deri.cqels.data.Mapping;
import org.deri.cqels.engine.iterator.MappingIterator;
import org.deri.cqels.engine.iterator.NullMappingIter;
import com.hp.hpl.jena.sparql.algebra.op.OpExtend;
import com.hp.hpl.jena.sparql.core.VarExprList;

public class ExtendRouter extends OpRouter1 {

    private final VarExprList exprs;

    public ExtendRouter(ExecContext context, OpExtend op, OpRouter sub) {
        super(context, op, sub);
        this.exprs = op.getVarExprList();
    }

    @Override
    public void route(Mapping mapping) {
        _route(new ExtendMapping(context, mapping, exprs));
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
        context.policy().removeRouter(sub(), this);
    }
}
