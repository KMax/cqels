package org.deri.cqels.engine;

public class TimerVisitor implements RouterVisitor{

	public void visit(JoinRouter router) {
		//System.out.println("Hello JoinRouter");
		
	}

	public void visit(IndexedTripleRouter router) {
//		Window w = router.getWindow();
//		if (w instanceof RangeWindow) {
//			Purifier p = new Purifier((RangeWindow)w);
//			if (((RangeWindow)w).getSlide() > 0)
//				((RangeWindow) w).enableSlidePurifier(p);
//		}
	}

	public void visit(ProjectRouter router) {
		//System.out.println("Hello ProjectRouter");
		
	}

	public void visit(ThroughRouter router) {
		//System.out.println("Hello ThroughRouter");
		
	}

	public void visit(BDBGraphPatternRouter router) {
		//System.out.println("Hello BDBGraphPatternRouter");
		
	}

	public void visit(ExtendRouter router) {
		//System.out.println("Hello ExtendRouter");
		
	}

	public void visit(FilterExprRouter router) {
		//System.out.println("Hello FilterExpRouter");
		
	}

	public void visit(ContinuousConstruct router) {
		//System.out.println("Hello ConinuousConstructRouter");
		
	}

	public void visit(ContinuousSelect router) {
		//System.out.println("Hello ContinousSelectRouter");
	}

	public void visit(GroupRouter router) {
		//System.out.println("Hello GroupRouter");
		
	}

	public void visit(OpRouter router) {
		//System.out.println("Hello OpRouter");
	}

}
