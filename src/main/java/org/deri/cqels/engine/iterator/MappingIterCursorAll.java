package org.deri.cqels.engine.iterator;

import java.util.ArrayList;

import org.deri.cqels.data.Mapping;
import org.deri.cqels.engine.ExecContext;

import com.hp.hpl.jena.sparql.core.Var;
import com.sleepycat.je.CursorConfig;
import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.LockMode;
import com.sleepycat.je.OperationStatus;

public class MappingIterCursorAll extends MappingIterCursor {
	ArrayList<Var> vars;
	public MappingIterCursorAll(ExecContext context, Database database, ArrayList<Var> vars) {
		super(context, database);
		this.vars = vars;
		//_readNext();
	}
	
	@Override
	public void _readNext() {
		DatabaseEntry key = new DatabaseEntry(); 
		curEnt = new DatabaseEntry();
		if(cursor == null) {
			cursor = db.openCursor(null, CursorConfig.READ_COMMITTED);
		}
		if(!(cursor.getNext(key, curEnt, LockMode.DEFAULT) == OperationStatus.SUCCESS)) {
			curEnt = null;
		}
		//System.out.println("read Next not null");
	}

	@Override
	protected Mapping moveToNextMapping() {
		Mapping mapping = null;
		if(curEnt != null) {
			mapping = org.deri.cqels.util.Utils.data2Mapping(context, curEnt, vars);
			_readNext();
		}
		return mapping;
	}
	
	

}
