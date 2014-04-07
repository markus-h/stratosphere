package eu.stratosphere.api.java;

import java.util.HashMap;
import java.util.Map;

import eu.stratosphere.api.java.io.CsvInputFormat;
import eu.stratosphere.api.java.operators.DataSource;
import eu.stratosphere.api.java.tuple.Tuple3;
import eu.stratosphere.api.java.typeutils.TupleTypeInfo;

public class NamedDataSet {

	private Map<String, Class<?>> dataItems;
	
	private final ExecutionEnvironment context;
	
	public NamedDataSet(ExecutionEnvironment context) {
		this.context = context;
		dataItems = new HashMap<String, Class<?>>();
	}
	
	public NamedDataSet(ExecutionEnvironment context, Map<String, Class<?>> dataItems) {
		this.context = context;
		this.dataItems = new HashMap<String, Class<?>>(dataItems);
	}
	
	public <T0> void addItem(String name, Class<T0> type) {
		this.dataItems.put(name, type);
	}
	
	public <T0, T1, T2> DataSet<Tuple3<T0, T1, T2>> types(Class<T0> type0, Class<T1> type1, Class<T2> type2) {
		TupleTypeInfo<Tuple3<T0, T1, T2>> types = TupleTypeInfo.getBasicTupleTypeInfo(type0, type1, type2);
		return new DataSet<Tuple3<T0, T1, T2>>(context, types);
	}
}
