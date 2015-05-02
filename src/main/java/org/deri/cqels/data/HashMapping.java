package org.deri.cqels.data;

import com.hp.hpl.jena.sparql.core.Var;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import org.apache.jena.atlas.iterator.IteratorConcat;
import org.deri.cqels.engine.ExecContext;

public class HashMapping extends MappingBase {

    HashMap<Var, Long> hMap;

    public HashMapping(ExecContext context, HashMap<Var, Long> hMap) {
        super(context);
        this.hMap = hMap;
    }

    public HashMapping(ExecContext context, HashMap<Var, Long> hMap, Mapping parent) {
        super(context, parent);
        this.hMap = hMap;
    }

    @Override
    public long get(Var var) {
        if (hMap != null) {
            Long value = hMap.get(var);
            if (value != null) {
                return value;
            }
        }

        return super.get(var);
    }

    @Override
    public Iterator<Var> vars() {
        return IteratorConcat.concat(hMap.keySet().iterator(), super.vars());
    }

    @Override
    public Long put(Var key, Long value) {
        return hMap.put(key, value);
    }

    @Override
    public void putAll(Map<? extends Var, ? extends Long> m) {
        hMap.putAll(m);
        //TODO : check if the pair exists in parents
    }

    @Override
    public boolean containsKey(Object key) {
        if (hMap.containsKey(key)) {
            return true;
        }
        return super.containsKey(key);
    }

    @Override
    public boolean containsValue(Object value) {
        if (hMap.containsValue(value)) {
            return true;
        }
        return super.containsValue(value);
    }

    @Override
    public boolean isEmpty() {
        if (hMap.size() > 0) {
            return true;
        }
        return super.isEmpty();
    }

    @Override
    public boolean isCompatible(Mapping mapping) {
        //for(Iterator<Entry<Var, Long>>itr=hMap.entrySet().iterator();itr.hasNext();){
        for (Entry<Var, Long> entry : hMap.entrySet()) {
            //Entry<Var, Long> entry=itr.next();
            if (mapping.get(entry.getKey()) >= 0
                    && mapping.get(entry.getKey()) != entry.getValue()) {
                return false;
            }
        }
        return super.isCompatible(mapping);
    }

    @Override
    public int size() {
        // TODO duplicated vars
        return hMap.size() + super.size();
    }
}
