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
package eu.stratosphere.example.java.nameddataset;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import eu.stratosphere.api.java.DataSet;
import eu.stratosphere.api.java.ExecutionEnvironment;
import eu.stratosphere.api.java.NamedDataSet;
import eu.stratosphere.api.java.aggregation.Aggregations;
import eu.stratosphere.api.java.functions.FlatMapFunction;
import eu.stratosphere.api.java.functions.ReduceFunction;
import eu.stratosphere.api.java.tuple.*;
import eu.stratosphere.test.javaApiOperators.ReduceITCase.Tuple3Reduce;
import eu.stratosphere.test.javaApiOperators.util.CollectionDataSets;
import eu.stratosphere.util.Collector;


@SuppressWarnings("serial")
public class DevTests {
	
	public static final class Tokenizer extends FlatMapFunction<String, Tuple2<String, Integer>> {

		@Override
		public void flatMap(String value, Collector<Tuple2<String, Integer>> out) {
			String[] tokens = value.toLowerCase().split("\\W");
			for (String token : tokens) {
				if (token.length() > 0) {
					out.collect(new Tuple2<String, Integer>(token, 1));
				}
			}
		}
	}
	
	
	public static void main(String[] args) throws Exception {
		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		
		DataSet<Tuple3<Integer, Long, String>> ds = get3TupleDataSet(env);
		NamedDataSet nds = ds.named("Eins", "Zwei", "Drei");
		DataSet<Tuple3<Integer, Long, String>> reduceDs = ds.
				groupBy(1).reduce(new Tuple3Reduce("B-)"));
		
		//reduceDs.writeAsCsv(resultPath);
		reduceDs.print();
		env.execute();
		
		// return expected result
//		return "1,1,Hi\n" +
//				"5,2,B-)\n" +
//				"15,3,B-)\n" +
//				"34,4,B-)\n" +
//				"65,5,B-)\n" +
//				"111,6,B-)\n";
	}
	
	public static DataSet<Tuple3<Integer, Long, String>> get3TupleDataSet(ExecutionEnvironment env) {
		
		List<Tuple3<Integer, Long, String>> data = new ArrayList<Tuple3<Integer, Long, String>>();
		data.add(new Tuple3<Integer, Long, String>(1,1l,"Hi"));
		data.add(new Tuple3<Integer, Long, String>(2,2l,"Hello"));
		data.add(new Tuple3<Integer, Long, String>(3,2l,"Hello world"));
		data.add(new Tuple3<Integer, Long, String>(4,3l,"Hello world, how are you?"));
		data.add(new Tuple3<Integer, Long, String>(5,3l,"I am fine."));
		data.add(new Tuple3<Integer, Long, String>(6,3l,"Luke Skywalker"));
		data.add(new Tuple3<Integer, Long, String>(7,4l,"Comment#1"));
		data.add(new Tuple3<Integer, Long, String>(8,4l,"Comment#2"));
		data.add(new Tuple3<Integer, Long, String>(9,4l,"Comment#3"));
		data.add(new Tuple3<Integer, Long, String>(10,4l,"Comment#4"));
		data.add(new Tuple3<Integer, Long, String>(11,5l,"Comment#5"));
		data.add(new Tuple3<Integer, Long, String>(12,5l,"Comment#6"));
		data.add(new Tuple3<Integer, Long, String>(13,5l,"Comment#7"));
		data.add(new Tuple3<Integer, Long, String>(14,5l,"Comment#8"));
		data.add(new Tuple3<Integer, Long, String>(15,5l,"Comment#9"));
		data.add(new Tuple3<Integer, Long, String>(16,6l,"Comment#10"));
		data.add(new Tuple3<Integer, Long, String>(17,6l,"Comment#11"));
		data.add(new Tuple3<Integer, Long, String>(18,6l,"Comment#12"));
		data.add(new Tuple3<Integer, Long, String>(19,6l,"Comment#13"));
		data.add(new Tuple3<Integer, Long, String>(20,6l,"Comment#14"));
		data.add(new Tuple3<Integer, Long, String>(21,6l,"Comment#15"));
		
		Collections.shuffle(data);
		
		return env.fromCollection(data);
	}
	
	public static class Tuple3Reduce extends ReduceFunction<Tuple3<Integer, Long, String>> {
		private static final long serialVersionUID = 1L;
		private final Tuple3<Integer, Long, String> out = new Tuple3<Integer, Long, String>();
		private final String f2Replace;
		
		public Tuple3Reduce() { 
			this.f2Replace = null;
		}
		
		public Tuple3Reduce(String f2Replace) { 
			this.f2Replace = f2Replace;
		}
		

		@Override
		public Tuple3<Integer, Long, String> reduce(
				Tuple3<Integer, Long, String> in1,
				Tuple3<Integer, Long, String> in2) throws Exception {

			if(f2Replace == null) {
				out.setFields(in1.f0+in2.f0, in1.f1, in1.f2);
			} else {
				out.setFields(in1.f0+in2.f0, in1.f1, this.f2Replace);
			}
			return out;
		}
	}
}
