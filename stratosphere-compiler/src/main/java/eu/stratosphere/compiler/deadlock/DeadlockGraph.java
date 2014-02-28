package eu.stratosphere.compiler.deadlock;

import java.util.Collection;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.Queue;
import java.util.Set;

import eu.stratosphere.compiler.plan.PlanNode;

public class DeadlockGraph {
	
	public Set<DeadlockVertex> vertices;
	
	public DeadlockGraph() {
		this.vertices = new HashSet<DeadlockVertex>();
	}
	   
	public Set<DeadlockVertex> vertices() {
		return vertices;
	}
	   
	public DeadlockVertex addVertex(PlanNode original) {
		
		DeadlockVertex v = new DeadlockVertex(original);
		this.vertices.add(v);
		return v;
	}
	
	public void addEdge(PlanNode source, PlanNode destination) {
		
		DeadlockVertex dest = null;
		for(DeadlockVertex v : vertices) {
			if(v.getOriginal().equals(destination))
				dest = v;
		}
		
		for(DeadlockVertex v : vertices) {
			if(v.getOriginal().equals(source)) {
				v.addEdge(dest);
			}
		}
		
	}
	   
	public long size() {
		return vertices.size();
	}
	
	public String toString() {
		StringBuilder out = new StringBuilder();
		out.append("------------ GRAPH ------------\n");
		for (DeadlockVertex n : vertices) {
			out.append("Node " +n+"_\n");
			for(DeadlockEdge a: n.getOutEdges()) {
				out.append("\t->"+a.getDestination()+"\n");
			}
			out.append("\n");
		}
		
		return out.toString();
	}

	public boolean hasCycle() {
		
	   
	   Collection <DeadlockVertex> vertexCollect = this.vertices();
	   
	   Queue <DeadlockVertex> q; // Queue will store vertices that have in-degree of zero
	
	   // Calculate the in-degree of all vertices
	   for (DeadlockVertex v: vertexCollect)
	      v.setInDegree(0);
	   
	   for (DeadlockVertex v: vertexCollect) {
	      for(DeadlockEdge edge : v.getOutEdges())
	         edge.getDestination().setInDegree(edge.getDestination().getInDegree()+1);
	   }
	
	   // Find all vertices with in-degree == 0 and put in queue 
	   q = new LinkedList<DeadlockVertex>();
	   for (DeadlockVertex v : vertexCollect) {
	      if (v.getInDegree() == 0)
	         q.offer(v);
	   }
	   
	   while (!q.isEmpty()) {
		   
		   DeadlockVertex v = q.poll();
		   this.vertices.remove(v);
		   
		   for (DeadlockEdge e: v.getOutEdges()) {
			   
			   DeadlockVertex w = e.getDestination();
			   w.setInDegree(w.getInDegree() - 1);
			   
			   if(w.getInDegree() == 0) {
				   q.offer(w);
			   }
		   }
	   }
	   
	   if (!vertexCollect.isEmpty() ){
	      return true;  //Cycle found
	   }
	   
	   return false;
	}

	
}
