package eu.stratosphere.api.java.record.operators;

import java.io.Serializable;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import eu.stratosphere.api.common.aggregators.Aggregator;
import eu.stratosphere.api.common.aggregators.ConvergenceCriterion;
import eu.stratosphere.api.common.operators.BulkIteration;
import eu.stratosphere.api.common.operators.Operator;
import eu.stratosphere.api.java.record.functions.MapFunction;
import eu.stratosphere.types.IntValue;
import eu.stratosphere.types.Record;
import eu.stratosphere.util.Collector;

public class BulkIterationOperator extends BulkIteration {

	/**
	 * @param name
	 */
	public BulkIterationOperator(String name) {
		super(name);
	}
	
	
	/**
	 * @param criterion
	 */
	public void setTerminationCriterion(Operator criterion) {
		// Mapper to aggregate number of matching fields
		MapOperator mapper = MapOperator.builder(new TerminationCriterionMapper())
				.input(criterion)
				.name("Termination Criterion Aggregation Wrapper")
				.build();
		
		this.terminationCriterion = mapper;
		
		this.getAggregators().registerAggregationConvergenceCriterion("default.aggregator", TerminationCriterionAggregator.class, TerminationCriterionAggregationConvergence.class);
	}
	
	public static class TerminationCriterionMapper extends MapFunction implements Serializable {
		private static final long serialVersionUID = 1L;
		
		@Override
		public void map(Record record, Collector<Record> collector) {
			
			TerminationCriterionAggregator aggregator = (TerminationCriterionAggregator) getIterationRuntimeContext().<IntValue>getIterationAggregator("default.aggregator");
			
			aggregator.aggregate(1);
		}
	}
	
	public static class TerminationCriterionAggregator implements Aggregator<IntValue> {

		private int count = 0;

		@Override
		public IntValue getAggregate() {
			return new IntValue(count);
		}

		public void aggregate(int count) {
			count += count;
		}

		@Override
		public void aggregate(IntValue count) {
			this.count += count.getValue();
		}

		@Override
		public void reset() {
			count = 0;
		}
	}

	public static class TerminationCriterionAggregationConvergence implements ConvergenceCriterion<IntValue> {

		private static final Log log = LogFactory.getLog(TerminationCriterionAggregationConvergence.class);
		
		private int lastCount = 0;

		@Override
		public boolean isConverged(int iteration, IntValue countAggregate) {
			int count = countAggregate.getValue();

			if (log.isInfoEnabled()) {
				log.info("Stats in iteration [" + iteration + "]: " + count);
			}

			if(count == lastCount) {
				return true;
			}
			else {
				lastCount = count;
				return false;
			}
		}
	}
}

