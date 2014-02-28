package eu.stratosphere.example.java.record.deadlock;

import java.util.StringTokenizer;

import eu.stratosphere.api.common.Plan;
import eu.stratosphere.api.common.Program;
import eu.stratosphere.api.common.operators.FileDataSink;
import eu.stratosphere.api.common.operators.FileDataSource;
import eu.stratosphere.api.java.record.functions.JoinFunction;
import eu.stratosphere.api.java.record.functions.MapFunction;
import eu.stratosphere.api.java.record.io.CsvOutputFormat;
import eu.stratosphere.api.java.record.io.TextInputFormat;
import eu.stratosphere.api.java.record.operators.JoinOperator;
import eu.stratosphere.api.java.record.operators.MapOperator;
import eu.stratosphere.client.LocalExecutor;
import eu.stratosphere.example.java.record.wordcount.WordCount;
import eu.stratosphere.nephele.client.JobExecutionResult;
import eu.stratosphere.types.IntValue;
import eu.stratosphere.types.Record;
import eu.stratosphere.types.StringValue;
import eu.stratosphere.util.Collector;

public class Deadlock implements Program {
	
	/**
	 * 
	 */
	private static final long serialVersionUID = 5224160767099401577L;


	public static class TokenizeLine extends MapFunction {
		private static final long serialVersionUID = 1L;

		@Override
		public void map(Record record, Collector<Record> collector) {
			// get the first field (as type StringValue) from the record
			String line = record.getField(0, StringValue.class).getValue();

			// normalize the line
			line = line.replaceAll("\\W+", " ").toLowerCase();
			
			// tokenize the line
			StringTokenizer tokenizer = new StringTokenizer(line);
			while (tokenizer.hasMoreTokens()) {
				String word = tokenizer.nextToken();
				
				// we emit a (word, 1) pair 
				collector.collect(new Record(new StringValue(word), new IntValue(1)));
			}
		}
	}
	
	public static class Joiner extends JoinFunction {

		/**
		 * 
		 */
		private static final long serialVersionUID = -9153041336999664506L;

		@Override
		public void join(Record value1, Record value2, Collector<Record> out)
				throws Exception {
			
			System.out.println(value1.getField(0, StringValue.class));
			out.collect(new Record(value1.getField(0, StringValue.class), new IntValue(1)));
			
		}
		
	}
	
	
	@Override
	public Plan getPlan(String... args) {
		// parse job parameters
		int numSubTasks   = 1;
		String dataInput = "file:\\D:\\Devel\\hamlet.txt";
		String output    = "file:\\D:\\Devel\\deadlock.txt";
		String output2    = "file:\\D:\\Devel\\deadlock2.txt";

		FileDataSource sourceLeft = new FileDataSource(new TextInputFormat(), dataInput, "Input Lines Left");
		FileDataSource sourceRight = new FileDataSource(new TextInputFormat(), dataInput, "Input Lines Rights");
		
		
		MapOperator mapperLeft = MapOperator.builder(new TokenizeLine())
			.input(sourceLeft)
			.name("Tokenize Lines Left")
			.build();
		
		
		MapOperator mapperRight = MapOperator.builder(new TokenizeLine())
				.input(sourceRight)
				.name("Tokenize Lines Right")
				.build();
		
		JoinOperator joinerLeft = JoinOperator.builder(new Joiner(), StringValue.class, 0, 0)
				.input1(mapperLeft)
				.input2(mapperRight)
				.name("Joiner Left")
				.build();
		
		JoinOperator joinerRight = JoinOperator.builder(new Joiner(), StringValue.class, 0, 0)
				.input1(mapperRight)
				.input2(mapperLeft)
				.name("Joiner Right")
				.build();
		
		@SuppressWarnings("unchecked")
		FileDataSink out = new FileDataSink(new CsvOutputFormat("\n", " ", StringValue.class, IntValue.class), output, joinerLeft, "Deadlock Example Left");
		FileDataSink out2 = new FileDataSink(new CsvOutputFormat("\n", " ", StringValue.class, IntValue.class), output2, joinerRight, "Deadlock Example Right");
	
		
		Plan plan = new Plan(out, "WordCount Example");
		plan.addDataSink(out2);
		plan.setDefaultParallelism(numSubTasks);
		return plan;
	}
	
	public static void main(String[] args) throws Exception {
		Deadlock wc = new Deadlock();
		
		Plan plan = wc.getPlan(args);
		
		// This will execute the word-count embedded in a local context. replace this line by the commented
		// succeeding line to send the job to a local installation or to a cluster for execution
		JobExecutionResult result = LocalExecutor.execute(plan);
		System.err.println("Total runtime: " + result.getNetRuntime());
//		PlanExecutor ex = new RemoteExecutor("localhost", 6123, "stratosphere-java-examples-0.4-WordCount.jar");
//		ex.executePlan(plan);
	}
	
}
