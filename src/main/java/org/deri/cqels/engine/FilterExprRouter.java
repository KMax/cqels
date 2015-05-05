package org.deri.cqels.engine;

import org.deri.cqels.data.Mapping;
import org.deri.cqels.data.Mapping2Binding;
import org.deri.cqels.data.MappingWrapped;
import org.deri.cqels.engine.iterator.MappingIterFilterExpr;
import org.deri.cqels.engine.iterator.MappingIterator;
import com.hp.hpl.jena.sparql.algebra.op.OpFilter;
import com.hp.hpl.jena.sparql.expr.ExprList;

/**
 * This class implements the router with filter operator
 *
 * @author Danh Le Phuoc
 * @author Chan Le Van
 * @organization DERI Galway, NUIG, Ireland www.deri.ie
 * @email danh.lephuoc@deri.org
 * @email chan.levan@deri.org
 * @see OpRouter1
 */
public class FilterExprRouter extends OpRouter1 {

    private final ExprList expList;

    public FilterExprRouter(ExecContext context, OpFilter op, OpRouter sub) {
        super(context, op, sub);
        expList = ((OpFilter) op).getExprs();
    }

    @Override
    public void route(Mapping mapping) {
        if (expList.isSatisfied(
                new Mapping2Binding(context, mapping), context.getARQExCtx())) {
            Mapping _mapping = new MappingWrapped(context, mapping);
            _route(_mapping);
        }
    }

    @Override
    public MappingIterator searchBuff4Match(Mapping mapping) {
        return new MappingIterFilterExpr(
                sub().searchBuff4Match(mapping), expList, context);
    }

    @Override
    public MappingIterator getBuff() {
        return new MappingIterFilterExpr(sub().getBuff(), expList, context);
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
