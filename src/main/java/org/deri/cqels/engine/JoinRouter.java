package org.deri.cqels.engine;

import org.deri.cqels.data.Mapping;
import org.deri.cqels.engine.iterator.MappingIterMatch;
import org.deri.cqels.engine.iterator.MappingIterator;
import org.deri.cqels.engine.iterator.MappingNestedLoopEqJoin;
import com.hp.hpl.jena.sparql.algebra.op.OpJoin;

/**
 * This class implements a binary router processing the join operator
 *
 * @author Danh Le Phuoc
 * @author Chan Le Van
 * @organization DERI Galway, NUIG, Ireland www.deri.ie
 * @email danh.lephuoc@deri.org
 * @email chan.levan@deri.org
 * @see OpRouter2
 */
public class JoinRouter extends OpRouter2 {

    public JoinRouter(ExecContext context, OpJoin op, 
            OpRouter left, OpRouter right) {
        super(context, op, left, right);
    }

    @Override
    public void route(Mapping mapping) {
        OpRouter childR = getOtherChildRouter(mapping);
        /*if(childR instanceof BDBGraphPatternRouter) {
         (new RoutingRunner(mapping.getCtx(), childR.searchBuff4Match(mapping),this)).start();
         return ;
         }*/
        MappingIterator itr = childR.searchBuff4Match(mapping);
        while (itr.hasNext()) {
            Mapping _mapping = itr.next();
            _route(_mapping);
        }
        itr.close();
    }

    public OpRouter getOtherChildRouter(Mapping mapping) {
        if (left().getId() == mapping.from().getId()) {
            return right();
        }
        return left();
    }

    @Override
    public MappingIterator getBuff() {
        OpRouter lR = left();
        OpRouter rR = right();
        //TODO: choose sort-merge join order
        if (lR instanceof IndexedTripleRouter) {
            new MappingNestedLoopEqJoin(context, rR, lR);
        }
        return new MappingNestedLoopEqJoin(context, lR, rR);
    }

    @Override
    public MappingIterator searchBuff4Match(Mapping mapping) {
        return new MappingIterMatch(context, getBuff(), mapping);
    }

    @Override
    public void visit(RouterVisitor rv) {
        rv.visit(this);
        left().visit(rv);
        right().visit(rv);
    }
    
    public void destroy() {
        context.policy().removeRouter(left(), this);
        context.policy().removeRouter(right(), this);
    }

}
