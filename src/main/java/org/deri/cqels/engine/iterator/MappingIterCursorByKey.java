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

public class MappingIterCursorByKey extends MappingIterCursor {
	DatabaseEntry key;
	Mapping mapping;
	ArrayList<Var> vars;
	public MappingIterCursorByKey(ExecContext context,Database db,DatabaseEntry key,Mapping mapping,ArrayList<Var> vars) {
		super(context, db);
		curEnt = new DatabaseEntry();
		this.key = key;
		this.mapping = mapping;
		this.vars = vars;
	}
	
	@Override
	public void _readNext() {
		// TODO Auto-generated method stub
		DatabaseEntry _key = new DatabaseEntry(); 
		curEnt = new DatabaseEntry();
		if(cursor == null) {
			cursor = db.openCursor(null, CursorConfig.READ_COMMITTED);
			if(!(cursor.getSearchKey(key, curEnt, LockMode.DEFAULT) == OperationStatus.SUCCESS))
					curEnt = null;
		}
		else{
			if(!(cursor.getNextDup(_key, curEnt, LockMode.DEFAULT) == OperationStatus.SUCCESS))
				curEnt = null;
		}
	}
	
	@Override
	protected Mapping moveToNextMapping() {
		 Mapping _mapping = null;
		if(curEnt != null) {
		   _mapping = org.deri.cqels.util.Utils.data2Mapping(context, curEnt, mapping, vars);
		    _readNext();
		}
		return _mapping;
	}
}
