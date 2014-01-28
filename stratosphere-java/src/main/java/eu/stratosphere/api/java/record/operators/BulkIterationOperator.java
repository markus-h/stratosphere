package eu.stratosphere.api.java.record.operators;

import java.io.Serializable;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import eu.stratosphere.api.common.aggregators.Aggregator;
import eu.stratosphere.api.common.aggregators.ConvergenceCriterion;
import eu.stratosphere.api.common.operators.BulkIteration;
import eu.stratosphere.api.common.operators.FileDataSink;
import eu.stratosphere.api.common.operators.GenericDataSink;
import eu.stratosphere.api.common.operators.Operator;
import eu.stratosphere.api.java.record.functions.MapFunction;
import eu.stratosphere.api.java.record.io.CsvOutputFormat;
import eu.stratosphere.api.java.record.io.DelimitedOutputFormat;
import eu.stratosphere.configuration.Configuration;
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
		
		//GenericDataSink out = new GenericDataSink(new DummyOutputFormat(), mapper, "Dummy Sink (never used)");
		FileDataSink out = new FileDataSink(new DummyOutputFormat(), "file:/C:/dummy", mapper, "Dummy Sink (never used)");
		
		this.terminationCriterion = mapper;
		
		this.getAggregators().registerAggregationConvergenceCriterion("terminationCriterion.aggregator", TerminationCriterionAggregator.class, TerminationCriterionAggregationConvergence.class);
	}
	
	public static class TerminationCriterionMapper extends MapFunction implements Serializable {
		private static final long serialVersionUID = 1L;
		private TerminationCriterionAggregator aggregator;
		
		@Override
		public void open(Configuration parameters) {
			
			aggregator = (TerminationCriterionAggregator) getIterationRuntimeContext().<IntValue>getIterationAggregator("terminationCriterion.aggregator");
		}
		
		@Override
		public void map(Record record, Collector<Record> collector) {
			
			System.out.println("MAPCRIT");
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
			
			System.out.println("ISCONVERGED");

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
	
	public static class DummyOutputFormat extends DelimitedOutputFormat {
		private static final long serialVersionUID = 1L;
		
		@Override
		public int serializeRecord(Record rec, byte[] target) throws Exception {
			return 0;
		}
	}
}

