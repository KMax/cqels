package org.deri.cqels.engine;
import java.util.TimerTask;
public class Purifier extends TimerTask {
	RangeWindow w;
	public Purifier(RangeWindow w) {
		this.w = w;
	}
	
	@Override
	public void run() {
		String message = "purge by slide at thread: " + Thread.currentThread().getId();
		//System.out.println(message);
		//Thread.currentThread().setPriority(Thread.MAX_PRIORITY);
		try {
			w.purge(w.getDuration() - w.getSlide(), message);
		} catch (Exception e) {
			e.printStackTrace();
		}
		
	}
}
