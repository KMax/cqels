package org.deri.cqels.lang.cqels;

import java.util.ArrayList;

public class DurationSet {
	ArrayList<Duration> durations;
	public DurationSet(){
		durations= new ArrayList<Duration>();
	}
	public  void add(Duration duration){
		durations.add(duration);
	}
	
	public long inNanoSec(){
		long nano=0;
		for(Duration duration:durations)
			nano+=duration.inNanosec();
		
		return nano;
			
	}
}
