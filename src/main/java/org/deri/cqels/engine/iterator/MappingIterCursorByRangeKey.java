package org.deri.cqels.engine.iterator;

import java.util.ArrayList;

import org.deri.cqels.data.Mapping;
import org.deri.cqels.engine.ExecContext;
import org.deri.cqels.util.Utils;

import com.hp.hpl.jena.sparql.core.Quad;
import com.hp.hpl.jena.sparql.core.Var;
import com.sleepycat.je.CursorConfig;
import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.LockMode;
import com.sleepycat.je.OperationStatus;

public class MappingIterCursorByRangeKey extends MappingIterCursor {
	DatabaseEntry key;
	Mapping mapping,_nextMapping;
	ArrayList<Var> vars;
	int idxLen;
	public MappingIterCursorByRangeKey(ExecContext context, Database db, DatabaseEntry key, 
			Mapping mapping, ArrayList<Var> vars, int idxLen) {
		super(context, db);		
		this.key = key;
		this.mapping = mapping;
		this.vars = vars;
		this.idxLen = idxLen;
	}
	
	@Override
	public void _readNext() {
		DatabaseEntry _key = new DatabaseEntry(key.getData());
		curEnt = new DatabaseEntry();
		if(cursor == null) {
			cursor = db.openCursor(null, CursorConfig.READ_COMMITTED);
			if((!(cursor.getSearchKeyRange(_key, curEnt, LockMode.DEFAULT) == OperationStatus.SUCCESS))
					|| Utils.outRange(curEnt, key, idxLen))			
				curEnt = null;
		}
		else {
			if((!(cursor.getNext(_key, curEnt, LockMode.DEFAULT) == OperationStatus.SUCCESS))
					|| Utils.outRange(curEnt, key, idxLen))			
				curEnt = null;
		}
	}
	
	@Override
	protected Mapping moveToNextMapping() {
		Mapping _mapping = null;
		if(curEnt != null){
			_mapping = Utils.data2Mapping(context, curEnt, mapping, vars);
			_readNext();
		}
		return _mapping;
	}	

}
