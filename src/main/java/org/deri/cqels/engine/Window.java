package org.deri.cqels.engine;

import com.sleepycat.je.Database;
/** 
 * This interface represents the abstract behaviors of a cqels window 
 * @author		Danh Le Phuoc
 * @author 		Chan Le Van
 * @organization DERI Galway, NUIG, Ireland  www.deri.ie
 * @email 	danh.lephuoc@deri.org
 * @email   chan.levan@deri.org
 */
public interface Window {

	/**
	 * This method will purify a number of expired elements in the window according to its characteristics
	 * (triple-based or time-based windows)
	 */
	public void purge();
	public void reportLatestTime(long t);
	/**each window has a buffer to store elements. This method sets it to current window
	 *@param db database-type buffer 
	 */
	public void setBuff(Database db);
	public Window clone();
}
