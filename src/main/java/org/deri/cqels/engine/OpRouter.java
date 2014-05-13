package org.deri.cqels.engine;

import org.deri.cqels.data.Mapping;
import org.deri.cqels.engine.iterator.MappingIterator;

import com.hp.hpl.jena.sparql.algebra.Op;
/**
 *This interface contains the behaviours of a router which is responsible to receive a mapping and route it
 *to the higher node in the router-node tree 
 * @author		Danh Le Phuoc
 * @author 		Chan Le Van
 * @organization DERI Galway, NUIG, Ireland  www.deri.ie
 * @email 	danh.lephuoc@deri.org
 * @email   chan.levan@deri.org
 */
public interface OpRouter {
	/**
	 * Each specific router contains an operator what is parsed from the query
	 * This method will get that op out 
	 */
	public Op getOp();
	/**
	 * Each specific router contains a data buffer in the form of mappings
	 * This method will get that data buffer out
	 */
	public MappingIterator getBuff();
	/**
	 * This method tries to search in the data mapping buffer whether there is any
	 * mapping that matches the specified mapping. 
	 * @param mapping the specified mapping
	 * @return a mapping iterator of matched mappings in buffer
	 */
	public MappingIterator searchBuff4Match(Mapping mapping);
	/**
	 * This method route the specified mapping to higher node in the router-node tree
	 * @param mapping the specified mapping
	 */
	public void route (Mapping mapping);
	/**
	 * Each specific router has a unique Id
	 * This method will get that Id out
	 */
	public int getId();
	/***/
	public void visit(RouterVisitor rv);
}
