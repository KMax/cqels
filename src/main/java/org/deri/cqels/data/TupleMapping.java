package org.deri.cqels.data;

import com.hp.hpl.jena.sparql.core.Var;
import com.hp.hpl.jena.tdb.store.NodeId;
import java.util.HashMap;
import java.util.Iterator;
import org.apache.jena.atlas.iterator.IteratorConcat;
import org.apache.jena.atlas.lib.Tuple;
import org.deri.cqels.engine.ExecContext;

public class TupleMapping extends MappingBase {

    HashMap<Var, Integer> var2Idx;
    Tuple<NodeId> tuple;

    public TupleMapping(ExecContext context, Tuple<NodeId> tuple, HashMap<Var, Integer> var2Idx) {
        super(context);
        this.var2Idx = var2Idx;
        this.tuple = tuple;
    }

    public TupleMapping(ExecContext context, Tuple<NodeId> tuple, HashMap<Var, Integer> var2Idx, Mapping parent) {
        super(context, parent);
        this.var2Idx = var2Idx;
        this.tuple = tuple;
    }

    @Override
    public long get(Var var) {
        if ((var2Idx != null) && var2Idx.get(var) != null) {
            NodeId value = tuple.get(var2Idx.get(var));
            if (value != null) {
                return value.getId();
            }
        }
        return super.get(var);
    }

    @Override
    public Iterator<Var> vars() {
        return IteratorConcat.concat(var2Idx.keySet().iterator(), super.vars());
    }

    @Override
    public boolean containsKey(Object key) {
        if (var2Idx.containsKey(key)) {
            return true;
        }
        return super.containsKey(key);
    }

    @Override
    public boolean containsValue(Object value) {
        if (tuple.asList().contains(value)) {
            return true;
        }
        //TODO : check if tuple contain value
        return super.containsValue(value);
    }

    @Override
    public boolean isEmpty() {
        if (tuple.size() > 0) {
            return true;
        }
        return super.isEmpty();
    }

    @Override
    public boolean isCompatible(Mapping mapping) {
        //for(Iterator<Entry<Var, Integer>>itr=var2Idx.entrySet().iterator();itr.hasNext();){
        for (Entry<Var, Integer> entry : var2Idx.entrySet()) {
            //Entry<Var, Integer> entry=itr.next();
            if (mapping.get(entry.getKey()) > 0
                    && mapping.get(entry.getKey()) != tuple.get(entry.getValue()).getId()) {
                return false;
            }
        }

        return super.isCompatible(mapping);
    }

    @Override
    public int size() {
        // TODO duplicated vars
        return tuple.size() + super.size();
    }
}
