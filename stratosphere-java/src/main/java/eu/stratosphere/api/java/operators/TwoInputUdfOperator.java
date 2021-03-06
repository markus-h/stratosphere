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

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import eu.stratosphere.api.common.operators.DualInputSemanticProperties;
import eu.stratosphere.api.java.DataSet;
import eu.stratosphere.api.java.typeutils.TypeInformation;
import eu.stratosphere.configuration.Configuration;

/**
 * The <tt>TwoInputUdfOperator</tt> is the base class of all binary operators that execute 
 * user-defined functions (UDFs). The UDFs encapsulated by this operator are naturally UDFs that
 * have two inputs (such as {@link JoinFunction} or {@link CoGroupFunction}).
 * <p>
 * This class encapsulates utilities for the UDFs, such as broadcast variables, parameterization 
 * through configuration objects, and semantic properties.
 *
 * @param <IN1> The data type of the first input data set.
 * @param <IN2> The data type of the second input data set.
 * @param <OUT> The data type of the returned data set.
 */
public abstract class TwoInputUdfOperator<IN1, IN2, OUT, O extends TwoInputUdfOperator<IN1, IN2, OUT, O>>
	extends TwoInputOperator<IN1, IN2, OUT, O> implements UdfOperator<O>
{
	private Configuration parameters;
	
	private Map<String, DataSet<?>> broadcastVariables;
	
	private DualInputSemanticProperties udfSemantics;
	
	// --------------------------------------------------------------------------------------------
	
	/**
	 * Creates a new operators with the two given data sets as inputs. The given result type
	 * describes the data type of the elements in the data set produced by the operator. 
	 * 
	 * @param input1 The data set for the first input.
	 * @param input2 The data set for the second input.
	 * @param resultType The type of the elements in the resulting data set.
	 */
	protected TwoInputUdfOperator(DataSet<IN1> input1, DataSet<IN2> input2, TypeInformation<OUT> resultType) {
		super(input1, input2, resultType);
	}
	
	// --------------------------------------------------------------------------------------------
	// Fluent API methods
	// --------------------------------------------------------------------------------------------
	
	@Override
	public O withParameters(Configuration parameters) {
		this.parameters = parameters;
		
		@SuppressWarnings("unchecked")
		O returnType = (O) this;
		return returnType;
	}
	
	@Override
	public O withBroadcastSet(DataSet<?> data, String name) {
		if (this.broadcastVariables == null) {
			this.broadcastVariables = new HashMap<String, DataSet<?>>();
		}
		
		this.broadcastVariables.put(name, data);
		
		@SuppressWarnings("unchecked")
		O returnType = (O) this;
		return returnType;
	}
	
	// --------------------------------------------------------------------------------------------
	// Accessors
	// --------------------------------------------------------------------------------------------
	
	@Override
	public Map<String, DataSet<?>> getBroadcastSets() {
		return this.broadcastVariables == null ?
				Collections.<String, DataSet<?>>emptyMap() :
				Collections.unmodifiableMap(this.broadcastVariables);
	}
	
	@Override
	public Configuration getParameters() {
		return this.parameters;
	}
	
	@Override
	public DualInputSemanticProperties getSematicProperties() {
		return this.udfSemantics;
	}
	
	/**
	 * Sets the semantic properties for the user-defined function (UDF). The semantic properties
	 * define how fields of tuples and other objects are modified or preserved through this UDF.
	 * The configured properties can be retrieved via {@link UdfOperator#getSematicProperties()}.
	 * 
	 * @param properties The semantic properties for the UDF.
	 * @see UdfOperator#getSematicProperties()
	 */
	public void setSemanticProperties(DualInputSemanticProperties properties) {
		this.udfSemantics = properties;
	}
}
