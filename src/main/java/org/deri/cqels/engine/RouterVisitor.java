package org.deri.cqels.engine;

public interface RouterVisitor {
	public void visit(JoinRouter router);
	public void visit(IndexedTripleRouter router);
	public void visit(ProjectRouter router);
	public void visit(ThroughRouter router);
	public void visit(BDBGraphPatternRouter router);
	public void visit(ExtendRouter router);
	public void visit(FilterExprRouter router);
	public void visit(ContinuousConstruct router);
	public void visit(ContinuousSelect router);
	public void visit(GroupRouter router);
	
	public void visit(OpRouter router);
	
}
