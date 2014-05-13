package org.deri.cqels.engine;
/** 
 * This class implements the all window - the triple-based window with all of elements valid 
 * @author		Danh Le Phuoc
 * @author 		Chan Le Van
 * @organization DERI Galway, NUIG, Ireland  www.deri.ie
 * @email 	danh.lephuoc@deri.org
 * @email   chan.levan@deri.org
 */
public class All extends RangeWindow {

	public All() {
		super(Long.MAX_VALUE);
	}

}
