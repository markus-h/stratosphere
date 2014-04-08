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

import eu.stratosphere.api.java.operators.Operator;
import eu.stratosphere.api.java.tuple.Tuple;
import eu.stratosphere.api.java.typeutils.TypeInformation;

public class NamedToTypedDataSet<T extends Tuple> extends Operator<T, NamedToTypedDataSet<T>> {
	
	private DataSet<?> derivedFrom;

	protected NamedToTypedDataSet(ExecutionEnvironment context,
			TypeInformation<T> type,
			DataSet<?> derivedFrom) {
		super(context, type);
		
		this.derivedFrom = derivedFrom;
	}

}
