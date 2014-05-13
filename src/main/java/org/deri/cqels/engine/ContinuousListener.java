package org.deri.cqels.engine;

import org.deri.cqels.data.Mapping;
/**
 * This interface contains behaviors used to process the mapping result 
 * @author		Danh Le Phuoc
 * @author 		Chan Le Van
 * @organization DERI Galway, NUIG, Ireland  www.deri.ie
 * @email 	danh.lephuoc@deri.org
 * @email   chan.levan@deri.org
 */
public interface ContinuousListener {
	/**
	 * This method processes the specified mapping which is the result of the query.
	 * The value of bindings in mapping are still integer
	 * @param mapping the mapping result parameterized by the engine
	 * */
	public void update(Mapping mapping);
}
