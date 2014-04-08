package eu.stratosphere.api.java;

import java.util.HashMap;
import java.util.Map;

import eu.stratosphere.api.java.operators.ProjectOperator.Projection;
import eu.stratosphere.api.java.tuple.Tuple;
import eu.stratosphere.api.java.tuple.Tuple2;
import eu.stratosphere.api.java.tuple.Tuple3;
import eu.stratosphere.api.java.typeutils.TupleTypeInfo;

public class NamedDataSet {

	private Map<String, Tuple2<Class<?>, Integer>> dataItems;
	
	private final ExecutionEnvironment context;
	
	private DataSet<? extends Tuple> derivedFrom;
	
	public NamedDataSet(ExecutionEnvironment context, DataSet<? extends Tuple> derivedFrom) {
		this.context = context;
		this.derivedFrom = derivedFrom;
		dataItems = new HashMap<String, Tuple2<Class<?>, Integer>>();
	}
	
	public NamedDataSet(ExecutionEnvironment context, Map<String, Tuple2<Class<?>, Integer>> dataItems, DataSet<? extends Tuple> derivedFrom) {
		this.context = context;
		this.derivedFrom = derivedFrom;
		this.dataItems = new HashMap<String, Tuple2<Class<?>, Integer>>(dataItems);
	}
	
	public <T0> void addItem(String name, Class<T0> type, int originalPosition) {
		this.dataItems.put(name, new Tuple2<Class<?>, Integer>(type, originalPosition));
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
		
		int[] fieldIndexes = new int[names.length];
		
		int i = 0;
		for(String name : names) {
			fieldIndexes[i++] = dataItems.get(name).f1;
		}
		
		return derivedFrom.project(fieldIndexes);
	}
	
//	public <T0, T1, T2> DataSet<Tuple3<T0, T1, T2>> types(Class<T0> type0, Class<T1> type1, Class<T2> type2) {
//		TupleTypeInfo<Tuple3<T0, T1, T2>> types = TupleTypeInfo.getBasicTupleTypeInfo(type0, type1, type2);
//		return new NamedToTypedDataSet<Tuple3<T0, T1, T2>>(context, types, this.derivedFrom);
//	}
}
