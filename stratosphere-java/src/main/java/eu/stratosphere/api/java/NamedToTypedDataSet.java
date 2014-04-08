package eu.stratosphere.api.java;

import eu.stratosphere.api.java.operators.Operator;
import eu.stratosphere.api.java.tuple.Tuple;
import eu.stratosphere.api.java.typeutils.TypeInformation;

public class NamedToTypedDataSet<T extends Tuple> extends Operator<T, NamedToTypedDataSet<T>> {
	
	private DataSet<? extends Tuple> derivedFrom;

	protected NamedToTypedDataSet(ExecutionEnvironment context,
			TypeInformation<T> type,
			DataSet<? extends Tuple> derivedFrom) {
		super(context, type);
		
		this.derivedFrom = derivedFrom;
	}

}
