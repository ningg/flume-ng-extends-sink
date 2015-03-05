package com.github.ningg.flume;

public class RTXRollbackException extends Exception {

	private static final long serialVersionUID = -5862280046404966754L;

	public RTXRollbackException(){
		
	}
	
	public RTXRollbackException(String msg){
		super(msg);
	}
	
	public RTXRollbackException(String msg, Throwable t){
		super(msg, t);
	}
	
	public RTXRollbackException(Throwable t){
		super(t);
	}
	
}
