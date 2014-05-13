package org.deri.cqels.engine.iterator;

import org.deri.cqels.data.Mapping;
import org.deri.cqels.data.Mapping2Binding;
import org.deri.cqels.engine.ExecContext;
import org.openjena.atlas.io.IndentedWriter;
import com.hp.hpl.jena.shared.PrefixMapping;
import com.hp.hpl.jena.sparql.engine.QueryIterator;
import com.hp.hpl.jena.sparql.engine.binding.Binding;
import com.hp.hpl.jena.sparql.serializer.SerializationContext;

public class QueryIterOnMappingIter implements QueryIterator{
	ExecContext context;
	MappingIterator mappingItr;
	public QueryIterOnMappingIter(ExecContext context, MappingIterator mappingItr){
	
		this.context=context;
		this.mappingItr=mappingItr;
	}

	public boolean hasNext() {
		//System.out.println("has Next Binding"+mappingItr.getClass() +" "+mappingItr.hasNext());
		return mappingItr.hasNext();
	}

	public Binding nextBinding() {
		//System.out.println(" moveToNextBinding");
		if(!mappingItr.hasNext()) return null;
		Mapping tmp=mappingItr.next();
		//System.out.println(" next Mapping"+tmp);
		return new Mapping2Binding(context,tmp);
	}

	
	public void close() {
		mappingItr.close();
	}

	public void cancel() {
		mappingItr.cancel();
	}
	
	public Binding next() {
		// TODO Auto-generated method stub
		return nextBinding();
	}
	public void remove() {
		// TODO Auto-generated method stub
		
	}
	public void output(IndentedWriter out, SerializationContext sCxt) {
		// TODO Auto-generated method stub
		
	}
	public String toString(PrefixMapping pmap) {
		// TODO Auto-generated method stub
		return null;
	}
	public void output(IndentedWriter out) {
		// TODO Auto-generated method stub
		
	}
	
	public void abort() {
		mappingItr.cancel();
	}
	

}
