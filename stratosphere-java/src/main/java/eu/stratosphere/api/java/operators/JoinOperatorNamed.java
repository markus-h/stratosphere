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
package eu.stratosphere.api.java.operators;

import eu.stratosphere.api.java.NamedDataSet;
import eu.stratosphere.api.java.functions.JoinFunction;
import eu.stratosphere.api.java.operators.translation.BinaryNodeTranslation;
import eu.stratosphere.api.java.tuple.Tuple;
import eu.stratosphere.api.java.tuple.Tuple2;
import eu.stratosphere.api.java.typeutils.TupleTypeInfo;
import eu.stratosphere.api.java.typeutils.TypeInformation;

/**
 *
 */
public abstract class JoinOperatorNamed extends NamedOperator {
	
	
	private final String [] keys1;
	private final String [] keys2;
	
	NamedDataSet input1;
	NamedDataSet input2;
	
	
	protected JoinOperatorNamed(NamedDataSet input1, NamedDataSet input2, 
			String[] keys1, String[] keys2,
			TypeInformation<Tuple> returnType)
	{
		
		super(input1.getExecutionEnvironment(), input1.getDerivedFrom());
		
		if (keys1 == null || keys2 == null)
			throw new NullPointerException();
		
		this.input1 = input1;
		this.input2 = input2;
		this.keys1 = keys1;
		this.keys2 = keys2;
	}
	
//	protected Keys<I1> getKeys1() {
//		return this.keys1;
//	}
//	
//	protected Keys<I2> getKeys2() {
//		return this.keys2;
//	}
	
//	protected JoinHint getJoinHint() {
//		return this.joinHint;
//	}
	
	// --------------------------------------------------------------------------------------------
	// special join types
	// --------------------------------------------------------------------------------------------
	
	public static class EquiJoinNamed<I1, I2, OUT> extends JoinOperatorNamed {
		
		private final JoinFunction<I1, I2, OUT> function;
		
		private boolean preserve1;
		private boolean preserve2;
		
		protected EquiJoinNamed(NamedDataSet input1, NamedDataSet input2, 
				String[] keys1, String[] keys2, JoinFunction<I1, I2, OUT> function,
				TypeInformation<Tuple> returnType)
		{
			super(input1, input2, keys1, keys2, returnType);
			
			if (function == null)
				throw new NullPointerException();
			
			this.function = function;
		}
		
		
		protected BinaryNodeTranslation translateToDataFlow() {

			// TODO
			return null;
		}
	}
	
	public static final class DefaultJoinNamed extends EquiJoinNamed {

		protected DefaultJoinNamed(NamedDataSet input1, NamedDataSet input2, 
				String[] keys1, String[] keys2)
		{
			super(input1, input2, keys1, keys2,
				new DefaultJoinFunction<Tuple, Tuple>(),
				new TupleTypeInfo<Tuple2<Tuple, Tuple>>(input1.getType(), input2.getType()));
		}
		
		
//		public <R> EquiJoin<I1, I2, R> with(JoinFunction<I1, I2, R> function) {
//			TypeInformation<R> returnType = TypeExtractor.getJoinReturnTypes(function);
//			return new EquiJoin<I1, I2, R>(getInput1(), getInput2(), getKeys1(), getKeys2(), function, returnType, getJoinHint());
//		}
	}
	
	
	// --------------------------------------------------------------------------------------------
	// Builder classes for incremental construction
	// --------------------------------------------------------------------------------------------
	
	public static final class JoinOperatorSetsNamed {
		
		private final NamedDataSet input1;
		private final NamedDataSet input2;
		
		public JoinOperatorSetsNamed(NamedDataSet input1, NamedDataSet input2) {
			this.input1 = input1;
			this.input2 = input2;
		}
		
		public JoinOperatorSetsPredicate where(String... keys1) {
			return new JoinOperatorSetsPredicate(keys1);
		}
		
	
		// ----------------------------------------------------------------------------------------
		
		public class JoinOperatorSetsPredicate {
			
			private final String[] keys1;
			
			private JoinOperatorSetsPredicate(String[] keys1) {
				
				this.keys1 = keys1;
			}
			
			
			public DefaultJoinNamed equalTo(String... keys2) {
				return createJoinOperator(keys2);
			}

			
			protected DefaultJoinNamed createJoinOperator(String[] keys2) {
				
				return new DefaultJoinNamed(input1, input2, keys1, keys2);
			}
		}
	}
	
	// --------------------------------------------------------------------------------------------
	//  default join functions
	// --------------------------------------------------------------------------------------------
	
	public static final class DefaultJoinFunction<T1, T2> extends JoinFunction<T1, T2, Tuple2<T1, T2>> {

		private static final long serialVersionUID = 1L;

		@Override
		public Tuple2<T1, T2> join(T1 first, T2 second) throws Exception {
			return new Tuple2<T1, T2>(first, second);
		}
	}
	
}
