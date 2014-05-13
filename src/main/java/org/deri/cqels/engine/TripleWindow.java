package org.deri.cqels.engine;

import com.sleepycat.je.Cursor;
import com.sleepycat.je.CursorConfig;
import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.LockMode;
import com.sleepycat.je.OperationStatus;
/** 
 * This class implements the triple-based window 
 * @author		Danh Le Phuoc
 * @author 		Chan Le Van
 * @organization DERI Galway, NUIG, Ireland  www.deri.ie
 * @email 	danh.lephuoc@deri.org
 * @email   chan.levan@deri.org
 */
public class TripleWindow implements Window {

    Database buff;
    long t;
    
    public TripleWindow( long t) {
    	this.t = t;
    }
	
	public void setBuff(Database db) { buff=db; }
	public void purge() {
		
		long count = buff.count();
		//System.out.println("c "+count);
		if(count > t) {
			long curT = System.nanoTime();
			Cursor cursor = buff.openCursor(null, CursorConfig.DEFAULT);
			DatabaseEntry key = new DatabaseEntry(new byte[0]);
			DatabaseEntry data = new DatabaseEntry(new byte[0]);
			while(count-- > t && (cursor.getNext(key, data, LockMode.DEFAULT) 
								  != OperationStatus.SUCCESS)) {
				//System.out.println(" delete triple");
				cursor.delete();
			}
			cursor.close();
			IndexedTripleRouter.accT += System.nanoTime() - curT;
		}
	}
	public void reportLatestTime(long t) {}
	public Window clone() {
		TripleWindow w = new TripleWindow(this.t);
		return w;
	}
}
