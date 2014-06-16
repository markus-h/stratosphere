/***********************************************************************************************************************
 *
 * Copyright (C) 2010-2013 by the Stratosphere project (http://stratosphere.eu)
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 *
 **********************************************************************************************************************/
package eu.stratosphere.nephele.jobmanager.iterations;

import java.io.IOException;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.google.common.base.Preconditions;

import eu.stratosphere.api.common.accumulators.Accumulator;
import eu.stratosphere.api.common.accumulators.ConvergenceCriterion;
import eu.stratosphere.nephele.executiongraph.ExecutionVertex;
import eu.stratosphere.nephele.executiongraph.ExecutionVertexID;
import eu.stratosphere.nephele.instance.AbstractInstance;
import eu.stratosphere.nephele.jobgraph.JobID;
import eu.stratosphere.nephele.jobmanager.accumulators.AccumulatorManager;
import eu.stratosphere.nephele.services.accumulators.AccumulatorEvent;
import eu.stratosphere.nephele.taskmanager.runtime.ExecutorThreadFactory;
import eu.stratosphere.pact.runtime.iterative.event.AllWorkersDoneEvent;
import eu.stratosphere.pact.runtime.iterative.event.WorkerDoneEvent;

/**
 * Manages the supersteps of one iteration. The JobManager is holding one IterationManager for every iteration that is 
 * currently running.
 *
 */
public class IterationManager {

	private static final Log log = LogFactory.getLog(IterationManager.class);
	
	private JobID jobId;
	
	private int iterationId;
	
	int numberOfEventsUntilEndOfSuperstep;
	
	int maxNumberOfIterations;
	
	int currentIteration = 1; // count starts at 1, not 0
	
	private int workerDoneEventCounter = 0;
	
	private ConvergenceCriterion<Object> convergenceCriterion;
	
	private String convergenceAccumulatorName;
	
	private boolean endOfSuperstep = false;
	
	private AccumulatorManager accumulatorManager;
	
	private CopyOnWriteArrayList<ExecutionVertex> executionVertices;
	
	private final ExecutorService executorService = Executors.newCachedThreadPool(ExecutorThreadFactory.INSTANCE);
	
	public IterationManager(JobID jobId, int iterationId, int numberOfEventsUntilEndOfSuperstep, int maxNumberOfIterations, 
			AccumulatorManager accumulatorManager, CopyOnWriteArrayList<ExecutionVertex> executionVertices) throws IOException {
		Preconditions.checkArgument(numberOfEventsUntilEndOfSuperstep > 0);
		this.jobId = jobId;
		this.iterationId = iterationId;
		this.numberOfEventsUntilEndOfSuperstep = numberOfEventsUntilEndOfSuperstep;
		this.maxNumberOfIterations = maxNumberOfIterations;
		this.accumulatorManager = accumulatorManager;
		this.executionVertices = executionVertices;
	}
	
	/**
	 * Is called once the JobManager receives a WorkerDoneEvent by RPC call from one node
	 */
	public synchronized void receiveWorkerDoneEvent(WorkerDoneEvent workerDoneEvent) {
		
		// sanity check
		if (this.endOfSuperstep) {
			throw new RuntimeException("Encountered WorderDoneEvent when still in End-of-Superstep status.");
		}
		
		workerDoneEventCounter++;
		
		// process accumulators
		this.accumulatorManager.processIncomingAccumulators(workerDoneEvent.getJobId(), workerDoneEvent.getAccumulators());
		
		// if all workers have sent their WorkerDoneEvent -> end of superstep
		if (workerDoneEventCounter % numberOfEventsUntilEndOfSuperstep == 0) {
			endOfSuperstep = true;
			handleEndOfSuperstep();
		}
	}
	
	/**
	 * Handles the end of one superstep. If convergence is reached it sends a termination request to all connected workers.
	 * If not it initializes the next superstep by sending an AllWorkersDoneEvent (with aggregators) to all workers.
	 */
	private void handleEndOfSuperstep() {
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

			// important for sanity checking
			resetEndOfSuperstep();
			
			final AllWorkersDoneEvent allWorkersDoneEvent = new AllWorkersDoneEvent(new AccumulatorEvent(this.jobId, this.accumulatorManager.getJobAccumulators(jobId), false));

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
			for (Accumulator<?, ?> agg : this.accumulatorManager.getJobAccumulators(jobId).values()) {
				agg.resetLocal();
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
	
	public void setConvergenceCriterion(String convergenceAccumulatorName, ConvergenceCriterion<Object> convergenceCriterion) {
		this.convergenceAccumulatorName = convergenceAccumulatorName;
		this.convergenceCriterion = convergenceCriterion;
	}
	
	/**
	 * Checks if either we have reached maxNumberOfIterations or if a associated ConvergenceCriterion is converged
	 */
	private boolean checkForConvergence() {
		if (maxNumberOfIterations == currentIteration) {
			if (log.isInfoEnabled()) {
				log.info("maximum number of iterations [" + currentIteration+ "] reached, terminating...");
			}
			return true;
		}

		if (convergenceAccumulatorName != null) {

			Accumulator<?, ? extends Object> acc = this.accumulatorManager.getJobAccumulators(jobId).get(convergenceAccumulatorName);
			//Aggregator<Value> aggregator = (Aggregator<Value>) this.aggregatorManager.getJobAggregator(jobId).get(convergenceAggregatorName);
			if (acc == null) {
				throw new RuntimeException("Error: Accumulator for convergence criterion was null.");
			}
			
			Object aggregate = acc.getLocalValue();

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
