package org.deri.cqels.engine.iterator;

import org.deri.cqels.engine.ExecContext;

public abstract class MappingIter extends MappingIteratorBase {
	protected ExecContext context;	

	public MappingIter(ExecContext context) {
		this.context = context;
	}
}
