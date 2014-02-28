package eu.stratosphere.compiler.deadlock;

public class DeadlockEdge {
	
	private DeadlockVertex destination;
	
	public DeadlockEdge( DeadlockVertex d ){
		destination = d;
	}
	   
	public DeadlockVertex getDestination() {
		return destination;
	}

	public void setDestination(DeadlockVertex destination) {
		this.destination = destination;
	}

}
