/***********************************************************************************************************************
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
 **********************************************************************************************************************/

package eu.stratosphere.nephele.jobmanager.iterations;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import eu.stratosphere.api.common.aggregators.Aggregator;
import eu.stratosphere.nephele.jobgraph.JobID;
import eu.stratosphere.types.Value;

public class AggregatorManager {

	// Map of aggregators belonging to recently started jobs
	private final Map<JobID, JobAggregators> aggregators = new ConcurrentHashMap<JobID, JobAggregators>();

	private final LinkedList<JobID> lru = new LinkedList<JobID>();
	private int maxEntries;

	public AggregatorManager(int maxEntries) {
		this.maxEntries = maxEntries;
	}
	
	public void addAggregator(JobID jobID, String name, Aggregator<?> aggregator) {
		
		JobAggregators aggregators = this.aggregators.get(jobID);
		if (aggregators == null) {
			aggregators = new JobAggregators();
		}
		aggregators.addAggregator(name, aggregator);
		
		cleanup(jobID);
	}

	/**
	 * Merges the new accumulators with the existing accumulators collected for
	 * the job.
	 */
	public void processIncomingAggregators(JobID jobID,
			String[] aggNames, Value[] aggregates) {
		
		if (aggNames.length != aggregates.length) {
			throw new RuntimeException("Inconsistent WorkerDoneEvent received!");
		}
			
		for (int i = 0; i < aggNames.length; i++) {
			@SuppressWarnings("unchecked")
			Aggregator<Value> aggregator = (Aggregator<Value>) this.aggregators.get(jobID).getAggregators().get(aggNames[i]);
			aggregator.aggregate(aggregates[i]);
		}

	}

	/**
	 * Returns all collected accumulators for the job. For efficiency the
	 * internal accumulator is returned, so please use it read-only.
	 */
	public Map<String, Aggregator<?>> getJobAggregator(JobID jobID) {
		
		JobAggregators jobAccumulators = this.aggregators.get(jobID);
		if (jobAccumulators == null) {
			return new HashMap<String, Aggregator<?>>();
		}
		return jobAccumulators.getAggregators();
	}

	/**
	 * Cleanup data for the oldest jobs if the maximum number of entries is
	 * reached.
	 */
	private void cleanup(JobID jobId) {
		if (!lru.contains(jobId)) {
			lru.addFirst(jobId);
		}
		if (lru.size() > this.maxEntries) {
			JobID toRemove = lru.removeLast();
			this.aggregators.remove(toRemove);
		}
	}
}
