package eu.stratosphere.nephele.jobmanager.iterations;

import java.io.IOException;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.google.common.base.Preconditions;

import eu.stratosphere.api.common.aggregators.Aggregator;
import eu.stratosphere.api.common.aggregators.ConvergenceCriterion;
import eu.stratosphere.nephele.execution.librarycache.LibraryCacheManager;
import eu.stratosphere.nephele.executiongraph.ExecutionVertex;
import eu.stratosphere.nephele.executiongraph.ExecutionVertexID;
import eu.stratosphere.nephele.instance.AbstractInstance;
import eu.stratosphere.nephele.jobgraph.JobID;
import eu.stratosphere.nephele.taskmanager.runtime.ExecutorThreadFactory;
import eu.stratosphere.pact.runtime.iterative.event.AllWorkersDoneEvent;
import eu.stratosphere.pact.runtime.iterative.event.WorkerDoneEvent;
import eu.stratosphere.types.Value;

/**
 * Manages the supersteps of one iteration
 *
 */
public class IterationManager {

	private static final Log log = LogFactory.getLog(IterationManager.class);
	
	private JobID jobId;
	
	private int iterationId;
	
	private int workerDoneEventCounter = 0;
	
	int numberOfEventsUntilEndOfSuperstep;
	
	int maxNumberOfIterations;
	
	int currentIteration = 0;
	
	private ClassLoader userCodeClassLoader;
	
	private ConvergenceCriterion<Value> convergenceCriterion;
	
	private String convergenceAggregatorName;
	
	private boolean endOfSuperstep = false;
	
	private AggregatorManager aggregatorManager;
	
	private CopyOnWriteArrayList<ExecutionVertex> executionVertices;
	
	private final ExecutorService executorService = Executors.newCachedThreadPool(ExecutorThreadFactory.INSTANCE);
	
	public IterationManager(JobID jobId, int iterationId, int numberOfEventsUntilEndOfSuperstep, int maxNumberOfIterations, 
			AggregatorManager aggregatorManager, CopyOnWriteArrayList<ExecutionVertex> executionVertices) throws IOException {
		Preconditions.checkArgument(numberOfEventsUntilEndOfSuperstep > 0);
		this.jobId = jobId;
		this.iterationId = iterationId;
		this.numberOfEventsUntilEndOfSuperstep = numberOfEventsUntilEndOfSuperstep;
		this.maxNumberOfIterations = maxNumberOfIterations;
		this.aggregatorManager = aggregatorManager;
		this.executionVertices = executionVertices;
		this.userCodeClassLoader =  LibraryCacheManager.getClassLoader(jobId);
	}
	
	public synchronized void receiveWorkerDoneEvent(WorkerDoneEvent workerDoneEvent) {
		
		if (this.endOfSuperstep) {
			throw new RuntimeException("Encountered WorderDoneEvent when still in End-of-Superstep status.");
		}
		
		workerDoneEventCounter++;
		
		String[] aggNames = workerDoneEvent.getAggregatorNames();
		Value[] aggregates = workerDoneEvent.getAggregates(userCodeClassLoader);

		if (aggNames.length != aggregates.length) {
			throw new RuntimeException("Inconsistent WorkerDoneEvent received!");
		}
		
		this.aggregatorManager.processIncomingAggregators(this.jobId, aggNames, aggregates);

		if (workerDoneEventCounter % numberOfEventsUntilEndOfSuperstep == 0) {
			endOfSuperstep = true;
			handleEndOfSuperstep();
		}
	}
	
	public void handleEndOfSuperstep() {
		if (log.isInfoEnabled()) {
			log.info("finishing iteration [" + currentIteration + "]");
		}

		if (checkForConvergence()) {
			if (log.isInfoEnabled()) {
				log.info("signaling that all workers are to terminate in iteration ["+ currentIteration + "]");
			}

			
			// Send termination to all workers
			for(ExecutionVertex ev : this.executionVertices) {
				
				final AbstractInstance instance = ev.getAllocatedResource().getInstance();
				if (instance == null) {
					log.error("Could not find instance to sent termination request for iteration.");
					return;
				}
				
				final ExecutionVertexID headVertexId = ev.getID();
				
				// send kill request
				final Runnable runnable = new Runnable() {
					@Override
					public void run() {
						try {
							instance.terminateIteration(headVertexId);
						} catch (IOException ioe) {
							log.error(ioe);
						}
					}
				};
				executorService.execute(runnable);
			}

		} else {
			if (log.isInfoEnabled()) {
				log.info("signaling that all workers are done in iteration [" + currentIteration+ "]");
			}

			resetEndOfSuperstep();
			
			final AllWorkersDoneEvent allWorkersDoneEvent = new AllWorkersDoneEvent(this.aggregatorManager.getJobAggregator(jobId));

			// Send start of next superstep to all workers
			for(ExecutionVertex ev : this.executionVertices) {
				
				final AbstractInstance instance = ev.getAllocatedResource().getInstance();
				if (instance == null) {
					log.error("Could not find instance to sent termination request for iteration.");
					return;
				}
				
				final ExecutionVertexID headVertexId = ev.getID();
				
				// send kill request
				final Runnable runnable = new Runnable() {
					@Override
					public void run() {
						try {
							instance.startNextSuperstep(headVertexId, allWorkersDoneEvent);
						} catch (IOException ioe) {
							log.error(ioe);
						}
					}
				};
				executorService.execute(runnable);
			}
			
			
			// reset all aggregators
			for (Aggregator<?> agg : this.aggregatorManager.getJobAggregator(jobId).values()) {
				agg.reset();
			}
			
			currentIteration++;
		}
	}
	
	public boolean isEndOfSuperstep() {
		return this.endOfSuperstep;
	}
	
	public void resetEndOfSuperstep() {
		this.endOfSuperstep = false;
	}
	
	public void setConvergenceCriterion(String convergenceAggregatorName, ConvergenceCriterion<Value> convergenceCriterion) {
		this.convergenceAggregatorName = convergenceAggregatorName;
		this.convergenceCriterion = convergenceCriterion;
	}
	
	private boolean checkForConvergence() {
		if (maxNumberOfIterations == currentIteration) {
			if (log.isInfoEnabled()) {
				log.info("maximum number of iterations [" + currentIteration+ "] reached, terminating...");
			}
			return true;
		}

		if (convergenceAggregatorName != null) {
			@SuppressWarnings("unchecked")
			Aggregator<Value> aggregator = (Aggregator<Value>) this.aggregatorManager.getJobAggregator(jobId).get(convergenceAggregatorName);
			if (aggregator == null) {
				throw new RuntimeException("Error: Aggregator for convergence criterion was null.");
			}
			
			Value aggregate = aggregator.getAggregate();

			if (convergenceCriterion.isConverged(currentIteration, aggregate)) {
				if (log.isInfoEnabled()) {
					log.info("convergence reached after [" + currentIteration + "] iterations, terminating...");
				}
				return true;
			}
		}
		
		return false;
	}
	
	public JobID getJobId() {
		return jobId;
	}

	public int getIterationId() {
		return iterationId;
	}
}
