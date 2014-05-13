package org.deri.cqels.engine.iterator;

import org.deri.cqels.data.Mapping;
import org.deri.cqels.data.Mapping2Binding;
import org.deri.cqels.engine.ExecContext;
import org.openjena.atlas.logging.Log;

import com.hp.hpl.jena.sparql.expr.ExprException;
import com.hp.hpl.jena.sparql.expr.ExprList;

public class MappingIterFilterExpr extends MappingIterProcessBinding {
	ExprList  exprList;
	public MappingIterFilterExpr(MappingIterator mIter,ExprList exprList, ExecContext context) {
		super(mIter, context);
		this.exprList = exprList;
	}

	@Override
	public Mapping accept(Mapping mapping) {
		try {
            if (exprList.isSatisfied(new Mapping2Binding(context, mapping), context.getARQExCtx())) {
                return mapping;
            }
            return null;
        } catch (ExprException ex) { 
        	// Some evaluation exception
            return null;
        }
        catch (Exception ex) {
            Log.warn(this, "General exception in " + exprList, ex);
            return null;
        }
	}

}
