package org.deri.cqels.data;



public class EnQuad {
	long gID,sID,pID,oID;
	long time;
	public EnQuad(long gID,long sID, long pID, long oID){
		this.gID=gID;
		this.sID=sID;
		this.pID=pID;
		this.oID=oID;
		time=System.nanoTime();
	}
	
	public long getGID(){
		return gID;
	}
	
	public long getSID(){
		return sID;
	}
	
	public long getPID(){
		return pID;
	}
	
	public long getOID(){
		return oID;
	}
	
	public long time(){ return time;};
}
