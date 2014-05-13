package org.deri.cqels.engine.iterator;

import org.deri.cqels.data.Mapping;


public class NullMappingIter extends MappingIter {
	private static NullMappingIter instance;
	public NullMappingIter() {
		super(null);
	}
	
	public static NullMappingIter instance() {
		if(instance==null) instance=new NullMappingIter(); 
		return instance;
	}
	
	@Override
	protected void closeIterator() {
		// TODO Auto-generated method stub

	}

	@Override
	protected boolean hasNextMapping() {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	protected Mapping moveToNextMapping() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	protected void requestCancel() {
		// TODO Auto-generated method stub

	}

}
