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
package eu.stratosphere.api.java;

import java.util.HashMap;
import java.util.Map;

import javax.naming.OperationNotSupportedException;

import eu.stratosphere.api.java.aggregation.Aggregations;
import eu.stratosphere.api.java.operators.JoinOperatorNamed.JoinOperatorSetsNamed;
import eu.stratosphere.api.java.operators.ProjectOperator.Projection;
import eu.stratosphere.api.java.typeutils.TypeInformation;

public class NamedDataSet {

	private Map<String, Class<?>> dataItems;
	
	private Map<String, Integer> originalTuplePosition;
	
	private final ExecutionEnvironment context;
	
	private DataSet<?> derivedFrom;
	
	public NamedDataSet(ExecutionEnvironment context, DataSet<?> derivedFrom) {
		this.context = context;
		this.derivedFrom = derivedFrom;
		dataItems = new HashMap<String, Class<?>>();
		originalTuplePosition = new HashMap<String, Integer>();
	}
	
	public NamedDataSet(ExecutionEnvironment context, Map<String, Class<?>> dataItems, DataSet<?> derivedFrom) {
		this.context = context;
		this.derivedFrom = derivedFrom;
		this.dataItems = new HashMap<String, Class<?>>(dataItems);
	}
	
	public void addItemFromTuple(String name, Class<?> type, int originalPosition) {
		this.dataItems.put(name, type);
		this.originalTuplePosition.put(name, originalPosition);
	}
	
	public TypeInformation<?> getType() {
		return this.derivedFrom.getType();
	}
	
	public ExecutionEnvironment getExecutionEnvironment() {
		return this.context;
	}
	
	public DataSet<?> getDerivedFrom() {
		return this.derivedFrom;
	}
	
//	public DataSet<? extends Tuple> typed(String name0, String name1, String name2) {
//		
//		return this.types(dataItems.get(name0), dataItems.get(name1), dataItems.get(name2));
//	}
	
	/**
	 * Used in form of .get(X,X,X).types(Y,Y,Y) to get a regular typed DataSet out of this NamedDataSet
	 * 
	 * @param names
	 * @return
	 */
	public Projection<?> get(String... names) {
		for(String name : names) {
			if(!dataItems.containsKey(name))
				throw new RuntimeException("Specified name for typing of NamedDataSet does not exist");
		}
		
		if(!this.getType().isTupleType())
			throw new RuntimeException("Currently NamedDataSets can only be derived from DataSet<? extends Tuple>");
		
		int[] fieldIndexes = new int[names.length];
		
		int i = 0;
		for(String name : names) {
			fieldIndexes[i++] = originalTuplePosition.get(name);
		}
		
		return derivedFrom.project(fieldIndexes);
	}
	
	public JoinOperatorSetsNamed join(NamedDataSet other) {
		return new JoinOperatorSetsNamed(this, other);
	}
	
	public NamedDataSet coGroup(NamedDataSet other) throws OperationNotSupportedException {
		throw new OperationNotSupportedException();
	}
	
	public NamedDataSet cross(NamedDataSet other) throws OperationNotSupportedException {
		throw new OperationNotSupportedException();
	}
	
	public NamedDataSet groupBy(String...fields) throws OperationNotSupportedException {
		throw new OperationNotSupportedException();
	}
	
	public NamedDataSet aggregate(Aggregations agg, int field) throws OperationNotSupportedException {
		throw new OperationNotSupportedException();
	}
	
//	public <T0, T1, T2> DataSet<Tuple3<T0, T1, T2>> types(Class<T0> type0, Class<T1> type1, Class<T2> type2) {
//		TupleTypeInfo<Tuple3<T0, T1, T2>> types = TupleTypeInfo.getBasicTupleTypeInfo(type0, type1, type2);
//		return new NamedToTypedDataSet<Tuple3<T0, T1, T2>>(context, types, this.derivedFrom);
//	}
}
