package org.deri.cqels.lang.cqels;

public class Duration {
	long nanoTime;
	String str;
	public Duration(){
		
	}
	
	public Duration(String str){
		this.str=str;
		nanoTime=parse(str);
		//System.out.println("window time "+nanoTime);
	}
	
	private long parse(String str){
		//System.out.println("window "+str);
		String number="0"; long unit=0;
		int l=str.length()-1;
		if(str.charAt(l)=='s'){
			if(str.charAt(l-1)=='m'){
				number=str.substring(0, l-1);
				unit=(long)1E6;
			}else if(str.charAt(l-1)=='n'){
				number=str.substring(0, l-1);
				unit=1;
			}
			else{
				number=str.substring(0, l);
				unit=(long)1E9;
			}
		}
		else if(str.charAt(l)=='m'){
			number=str.substring(0, l);
			unit=(long)60E9;
		}
		else if(str.charAt(l)=='h'){
			number=str.substring(0, l-1);
			unit=(long)3600E9;
		}
		else if(str.charAt(l)=='d'){
			number=str.substring(0, l-1);
			unit=((long)3600E9)*24;
		}
		
		return Long.parseLong(number)*unit;
	}
	public long inNanosec(){
		return nanoTime;
	}
}
