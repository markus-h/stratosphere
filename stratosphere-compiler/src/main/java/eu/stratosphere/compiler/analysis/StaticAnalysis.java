/***********************************************************************************************************************
 *
 * Copyright (C) 2010 by the Stratosphere project (http://stratosphere.eu)
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

package eu.stratosphere.compiler.analysis;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.codehaus.jackson.JsonGenerationException;
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.ObjectMapper;

import soot.Body;
import soot.Local;
import soot.PatchingChain;
import soot.Scene;
import soot.SootClass;
import soot.SootMethod;
import soot.Unit;
import soot.Value;
import soot.ValueBox;
import soot.jimple.AssignStmt;
import soot.jimple.CastExpr;
import soot.jimple.GotoStmt;
import soot.jimple.IfStmt;
import soot.jimple.InstanceInvokeExpr;
import soot.jimple.IntConstant;
import soot.jimple.NullConstant;
import soot.toolkits.graph.ExceptionalUnitGraph;
import soot.toolkits.scalar.LocalDefs;
import soot.toolkits.scalar.LocalUses;
import soot.toolkits.scalar.SimpleLocalDefs;
import soot.toolkits.scalar.SimpleLocalUses;
import soot.toolkits.scalar.UnitValueBoxPair;
import soot.util.Chain;
import eu.stratosphere.api.common.functions.Function;
//import eu.stratosphere.pact.common.stubs.Stub;
import eu.stratosphere.compiler.analysis.StubAnnotation.ImplicitOperation;
import eu.stratosphere.compiler.analysis.StubAnnotation.ImplicitOperation.ImplicitOperationMode;
import eu.stratosphere.compiler.analysis.StubAnnotation.OutCardBounds;
//import eu.stratosphere.pact.common.util.FieldSet;
import eu.stratosphere.api.common.operators.util.FieldSet;

/**
 * A class for running static code analysis on UDFs to determine the READ and
 * WRITE sets of those functions.
 * 
 * @author Aljoscha Krettek
 * 
 */
public class StaticAnalysis {
	private static final String GET_FIELD = "getField";
	private static final String SET_FIELD = "setField";
	private static final String SET_NULL = "setNull";
	private static final String UNION = "unionFields";
	private static final String COLLECT = "collect";
	private static final String ITERATOR = "next";

	private static final String MAP_METHOD_NAME = "map";
	private static final String REDUCE_METHOD_NAME = "reduce";
	private static final String COGROUP_METHOD_NAME = "coGroup";
	private static final String CROSS_METHOD_NAME = "cross";
	private static final String MATCH_METHOD_NAME = "match";

	private static final int UNARY_INPUT_POS = 0;
	private static final int UNARY_COLLECTOR_POS = 1;

	private static final int BINARY_LEFT_INPUT_POS = 0;
	private static final int BINARY_RIGHT_INPUT_POS = 1;
	private static final int BINARY_COLLECTOR_POS = 2;

	private LocalDefs useDef;
	private LocalUses defUse;

	private Unit collectorDef;
	private Body udfBody;
	private PatchingChain<Unit> units;
	private ExceptionalUnitGraph unitGraph;

	private Map<Unit, Integer> getStmts;
	private Map<Unit, Integer> inRecordDefs;
	private Map<Integer, Integer> readSet = new HashMap<Integer, Integer>();
	private Map<Unit, Map<Local, AttributeSets>> analyzed;

	// for use in json emitting
	private Map<Integer, String> emits;
	private Map<Unit, Map<String, Object>> modifications;

	private ImplicitOperation.ImplicitOperationMode leftImplOp;
	private ImplicitOperation.ImplicitOperationMode rightImplOp;
	private FieldSet writeSet;
	private FieldSet leftReadSet;
	private FieldSet leftExplicitProjectionSet;
	private FieldSet leftExplicitCopySet;
	private FieldSet rightReadSet;
	private FieldSet rightExplicitProjectionSet;
	private FieldSet rightExplicitCopySet;

	private int lowerBound = 1;
	private int upperBound = 1;

	/**
	 * Calculates the READ set and WRITE sets of the passed in UDF.
	 */
	public StaticAnalysis(Class<? extends Function> udf) {

		SootClass udfClass = null;
		// try {
		udfClass = Scene.v().loadClassAndSupport(udf.getName());
		Scene.v().loadBasicClasses();
		// } catch (RuntimeException e) {
		// System.out.println("Could not load class " + udf.getName() +
		// " with soot:");
		// e.printStackTrace();
		// return;
		// }

		List<SootMethod> methods = udfClass.getMethods();
		getStmts = new HashMap<Unit, Integer>();
		inRecordDefs = new HashMap<Unit, Integer>();
		analyzed = new HashMap<Unit, Map<Local, AttributeSets>>();

		for (SootMethod method : methods) {
			if (method.getName().equals(MAP_METHOD_NAME) || method.getName().equals(REDUCE_METHOD_NAME)
					|| method.getName().equals(COGROUP_METHOD_NAME) || method.getName().equals(CROSS_METHOD_NAME)
					|| method.getName().equals(MATCH_METHOD_NAME)) {
				udfBody = method.retrieveActiveBody();
				units = udfBody.getUnits();
				unitGraph = new ExceptionalUnitGraph(udfBody);
				useDef = new SimpleLocalDefs(unitGraph);
				defUse = new SimpleLocalUses(unitGraph, useDef);
			}

			if (method.getName().equals(MAP_METHOD_NAME)) {
				analyzeUnarySingleInput(udfClass, method);
				break;
			} else if (method.getName().equals(REDUCE_METHOD_NAME)) {
				analyzeUnaryMultInput(udfClass, method);
				break;
			} else if (method.getName().equals(COGROUP_METHOD_NAME)) {
				analyzeBinaryMultInput(udfClass, method);
				break;
			} else if (method.getName().equals(CROSS_METHOD_NAME)) {
				analyzeBinarySingleInput(udfClass, method);
				break;
			} else if (method.getName().equals(MATCH_METHOD_NAME)) {
				analyzeBinarySingleInput(udfClass, method);
				break;
			}
		}

		analyzeBounds();
	}

	private void analyzeBounds() {
		// the defaults, will be corrected when we find jumps that have targets
		// before/after emit stmts
		lowerBound = 1;
		upperBound = 1;

		List<Unit> emitStmts = new LinkedList<Unit>();
		for (Object o : defUse.getUsesOf(collectorDef)) {
			// should be true for getUsesOf
			assert (o instanceof UnitValueBoxPair);
			UnitValueBoxPair pair = (UnitValueBoxPair) o;
			Unit collectUnit = pair.getUnit();
			InstanceInvokeExpr collectCall = findInvokeExpr(collectUnit, COLLECT);
			if (collectCall == null) {
				continue;
			}
			emitStmts.add(collectUnit);
		}

		PatchingChain<Unit> units = udfBody.getUnits();
		for (Unit emitStmt : emitStmts) {
			// traverse unit list upwards until we find jump behind
			// the emit stmt or arrive at top
			Unit current = emitStmt;
			while (current != null) {
				if (current instanceof GotoStmt) {
					GotoStmt gotoStmt = (GotoStmt) current;
					if (units.follows(gotoStmt.getTarget(), emitStmt)) {
						lowerBound = 0;
						break;
					}
				}
				if (current instanceof IfStmt) {
					IfStmt ifStmt = (IfStmt) current;
					if (units.follows(ifStmt.getTarget(), emitStmt)) {
						lowerBound = 0;
						break;
					}
				}
				current = units.getPredOf(current);
			}

			// and now the same in the other direction for checking
			// whether we have a jump to before the emit
			current = emitStmt;
			while (current != null) {
				if (current instanceof GotoStmt) {
					GotoStmt gotoStmt = (GotoStmt) current;
					if (units.follows(emitStmt, gotoStmt.getTarget())) {
						upperBound = OutCardBounds.UNBOUNDED;
						break;
					}
				}
				if (current instanceof IfStmt) {
					IfStmt ifStmt = (IfStmt) current;
					if (units.follows(emitStmt, ifStmt.getTarget())) {
						upperBound = OutCardBounds.UNBOUNDED;
						break;
					}
				}
				current = units.getSuccOf(current);
			}
		}
	}

	private void analyzeUnarySingleInput(SootClass udfClass, SootMethod udfMethod) {
		Local inRecord = udfBody.getParameterLocal(UNARY_INPUT_POS);
		Local collector = udfBody.getParameterLocal(UNARY_COLLECTOR_POS);
		collectorDef = null;

		// first find the units that define the input record and the collector
		// parameter
		for (Unit unit : units) {
			for (ValueBox defBox : unit.getDefBoxes()) {
				if (defBox.getValue().equals(inRecord)) {
					inRecordDefs.put(unit, 0);
				}
				if (defBox.getValue().equals(collector)) {
					collectorDef = unit;
				}
			}
		}

		analyzeGenericUDF();
	}

	private void analyzeBinarySingleInput(SootClass udfClass, SootMethod udfMethod) {
		Local leftInRecord = udfBody.getParameterLocal(BINARY_LEFT_INPUT_POS);
		Local rightInRecord = udfBody.getParameterLocal(BINARY_RIGHT_INPUT_POS);
		Local collector = udfBody.getParameterLocal(BINARY_COLLECTOR_POS);
		collectorDef = null;

		// first find the units that define the input record and the collector
		// parameter
		for (Unit unit : units) {
			for (ValueBox defBox : unit.getDefBoxes()) {
				if (defBox.getValue().equals(leftInRecord)) {
					inRecordDefs.put(unit, 0);
				}
				if (defBox.getValue().equals(rightInRecord)) {
					inRecordDefs.put(unit, 1);
				}
				if (defBox.getValue().equals(collector)) {
					collectorDef = unit;
				}
			}
		}
		analyzeGenericUDF();
	}

	private void analyzeUnaryMultInput(SootClass udfClass, SootMethod udfMethod) {
		Local inIterator = udfBody.getParameterLocal(UNARY_INPUT_POS);
		Local collector = udfBody.getParameterLocal(UNARY_COLLECTOR_POS);
		Unit inIteratorDef = null;
		collectorDef = null;

		// first find the units that define the input record iterator and the
		// collector
		// parameter
		for (Unit unit : units) {
			for (ValueBox defBox : unit.getDefBoxes()) {
				if (defBox.getValue().equals(inIterator)) {
					inIteratorDef = unit;
				}
				if (defBox.getValue().equals(collector)) {
					collectorDef = unit;
				}
			}
		}

		// find all the statements where inIterator.next() is called, these are
		// the inRecordDefs
		for (Object o : defUse.getUsesOf(inIteratorDef)) {
			// should be true for getUsesOf
			assert (o instanceof UnitValueBoxPair);
			UnitValueBoxPair pair = (UnitValueBoxPair) o;
			Unit unit = pair.getUnit();
			InstanceInvokeExpr nextCall = findInvokeExpr(unit, ITERATOR);
			if (nextCall == null) {
				continue;
			}
			inRecordDefs.put(unit, 0);
		}

		addTransitiveDefs(inRecordDefs);

		analyzeGenericUDF();
	}

	private void analyzeBinaryMultInput(SootClass udfClass, SootMethod udfMethod) {
		Local leftInIterator = udfBody.getParameterLocal(BINARY_LEFT_INPUT_POS);
		Local rightInIterator = udfBody.getParameterLocal(BINARY_RIGHT_INPUT_POS);
		Local collector = udfBody.getParameterLocal(BINARY_COLLECTOR_POS);
		Unit leftInIteratorDef = null;
		Unit rightInIteratorDef = null;
		collectorDef = null;

		// first find the units that define the input record iterator and the
		// collector
		// parameter
		for (Unit unit : units) {
			for (ValueBox defBox : unit.getDefBoxes()) {
				if (defBox.getValue().equals(leftInIterator)) {
					leftInIteratorDef = unit;
				}
				if (defBox.getValue().equals(rightInIterator)) {
					rightInIteratorDef = unit;
				}
				if (defBox.getValue().equals(collector)) {
					collectorDef = unit;
				}
			}
		}

		// find all the statements where inIterator.next() is called, these are
		// the inRecordDefs
		for (Object o : defUse.getUsesOf(leftInIteratorDef)) {
			// should be true for getUsesOf
			assert (o instanceof UnitValueBoxPair);
			UnitValueBoxPair pair = (UnitValueBoxPair) o;
			Unit unit = pair.getUnit();
			InstanceInvokeExpr nextCall = findInvokeExpr(unit, ITERATOR);
			if (nextCall == null) {
				continue;
			}
			inRecordDefs.put(unit, 0);
		}
		for (Object o : defUse.getUsesOf(rightInIteratorDef)) {
			// should be true for getUsesOf
			assert (o instanceof UnitValueBoxPair);
			UnitValueBoxPair pair = (UnitValueBoxPair) o;
			Unit unit = pair.getUnit();
			InstanceInvokeExpr nextCall = findInvokeExpr(unit, ITERATOR);
			if (nextCall == null) {
				continue;
			}
			inRecordDefs.put(unit, 1);
		}

		addTransitiveDefs(inRecordDefs);

		analyzeGenericUDF();
	}

	private void addTransitiveDefs(Map<Unit, Integer> inRecordDefs) {
		// Also add all units to the inRecordDefs that are defs
		// of new values that use a value from the inRecordDefs.
		// This is necessary because Jimple does something like this:
		// $r0 = records.next()
		// r1 = $r0
		Iterator<Unit> it = inRecordDefs.keySet().iterator();
		while (it.hasNext()) {
			Unit unit = it.next();
			for (Object o : defUse.getUsesOf(unit)) {
				// should be true for getUsesOf
				assert (o instanceof UnitValueBoxPair);
				UnitValueBoxPair pair = (UnitValueBoxPair) o;
				Unit useUnit = pair.getUnit();
				if (useUnit instanceof AssignStmt) {
					AssignStmt assign = (AssignStmt) useUnit;
					if (assign.getRightOp() instanceof Local && !(inRecordDefs.containsKey(useUnit))) {
						inRecordDefs.put(useUnit, inRecordDefs.get(unit));
						// We do not want a ConcurrentModificationException, do
						// we? ... :D
						it = inRecordDefs.keySet().iterator();
					} else if (assign.getRightOp() instanceof CastExpr) {
						CastExpr cast = (CastExpr) assign.getRightOp();
						if (cast.getOp() instanceof Local && !(inRecordDefs.containsKey(useUnit))) {
							inRecordDefs.put(useUnit, inRecordDefs.get(unit));
							it = inRecordDefs.keySet().iterator();
						}
					}
				}
			}
		}
	}

	private AttributeSets analyzeGenericUDF() {
		emits = new HashMap<Integer, String>();
		modifications = new HashMap<Unit, Map<String, Object>>();
		// find all the statements where attributes are read from the inRecords
		// and update the READ set

		// getStmts is used later to determine whether the values of setField
		// calls come from getField calls
		getStmts = new HashMap<Unit, Integer>();
		for (Unit inRecordDef : inRecordDefs.keySet()) {
			for (Object o : defUse.getUsesOf(inRecordDef)) {
				// should be true for getUsesOf
				assert (o instanceof UnitValueBoxPair);
				UnitValueBoxPair pair = (UnitValueBoxPair) o;
				Unit getFieldUnit = pair.getUnit();
				InstanceInvokeExpr getFieldCall = findInvokeExpr(getFieldUnit, GET_FIELD);
				if (getFieldCall == null) {
					continue;
				}
				if (getFieldUnit instanceof AssignStmt) {
					// only add to the read set if the value can actually be
					// used
					getStmts.put(getFieldUnit, inRecordDefs.get(inRecordDef));
					readSet.put(getAttributePosition(getFieldCall, getFieldUnit), inRecordDefs.get(inRecordDef));
					Map<String, Object> mod = new HashMap<String, Object>();
					mod.put("type", "read");
					mod.put("attribute", getAttributePosition(getFieldCall, getFieldUnit));
					modifications.put(getFieldUnit, mod);
				}
			}
		}

		// now find all the emit statements and find out which
		// record they emit, it is assumed here that only local variables
		// are ever emitted, must add a preprocessing step to
		// ensure this for code that uses an instance field
		// for the output record

		// record defs that get emitted
		Set<Unit> outRecordDefs = new HashSet<Unit>();
		List<Unit> emitStmts = new LinkedList<Unit>();

		// first determine the defs of emitted records
		for (Object o : defUse.getUsesOf(collectorDef)) {
			// should be true for getUsesOf
			assert (o instanceof UnitValueBoxPair);
			UnitValueBoxPair pair = (UnitValueBoxPair) o;
			Unit collectUnit = pair.getUnit();
			InstanceInvokeExpr collectCall = findInvokeExpr(collectUnit, COLLECT);
			if (collectCall == null) {
				continue;
			}
			Local outRecord = (Local) collectCall.getArg(0);
			List<Unit> localOutRecordDefs = useDef.getDefsOfAt(outRecord, collectUnit);
			emitStmts.add(collectUnit);
			outRecordDefs.addAll(localOutRecordDefs);
		}

		if (emitStmts.size() <= 0) {
			return new AttributeSets();
		}
		Unit firstEmit = emitStmts.get(0);
		emitStmts.remove(0);
		InstanceInvokeExpr collectCall = findInvokeExpr(firstEmit, COLLECT);
		Local outRecord = (Local) collectCall.getArg(0);
		AttributeSets result = analyzeStmt(firstEmit, outRecord);
		// store them so that we can emit them as json if needed
		emits.put(indexOfUnitInChain(units, firstEmit), outRecord.toString());

		for (Unit emitStmt : emitStmts) {
			collectCall = findInvokeExpr(emitStmt, COLLECT);
			outRecord = (Local) collectCall.getArg(0);
			result.intersect(analyzeStmt(emitStmt, outRecord));
			// store them so that we can emit them as json if needed
			emits.put(indexOfUnitInChain(units, emitStmt), outRecord.toString());
		}

		// System.out.println("READ: " + readSet);
		// System.out.println(result);
		createLegacySets(result);
		return result;
	}

	/**
	 * Recursively analyzes a udf body, as outlined in the XLDI2012 paper.
	 */
	private AttributeSets analyzeStmt(Unit stmt, Local outRecord) {
		if (analyzed.containsKey(stmt) && analyzed.get(stmt).containsKey(outRecord)) {
			return analyzed.get(stmt).get(outRecord);
		}

		if (!analyzed.containsKey(stmt)) {
			analyzed.put(stmt, new HashMap<Local, AttributeSets>());
		}

		AttributeSets result = new AttributeSets();

		// already put it there as a dummy, otherwise we will
		// run into a stack overflow because we call getRealPreds later
		// before storing the result in analyzed
		// analyzed.put(key, result);

		if (inRecordDefs.containsKey(stmt)) {
			boolean isCorrectDef = false;
			for (ValueBox vb : stmt.getDefBoxes()) {
				if (vb.getValue().equals(outRecord)) {
					isCorrectDef = true;
				}
			}
			if (isCorrectDef) {
				result.origin.add(inRecordDefs.get(stmt));
				Map<String, Object> mod = new HashMap<String, Object>();
				mod.put("type", "origin");
				mod.put("record", outRecord.toString());
				mod.put("input", inRecordDefs.get(stmt));
				modifications.put(stmt, mod);
				analyzed.get(stmt).put(outRecord, result);
				return result;
			}
		}

		List<Unit> preds = getRealPreds(stmt);

		if (preds.size() <= 0) {
			analyzed.get(stmt).put(outRecord, result);
			return result;
		}

		InstanceInvokeExpr unionCall = findInvokeExpr(stmt, UNION);
		if (unionCall != null && unionCall.getBase().equals(outRecord)) {
			// first determine along path of original out record
			preds = getRealPreds(stmt);
			Unit firstPred = preds.get(0);
			preds.remove(0);
			AttributeSets result1 = analyzeStmt(firstPred, outRecord);
			for (Unit predStmt : preds) {
				result1.intersect(analyzeStmt(predStmt, outRecord));
			}

			// then for unioned out record
			Local unionedRecord = getUnionedValue(unionCall);
			preds = getRealPreds(stmt);
			firstPred = preds.get(0);
			preds.remove(0);
			AttributeSets result2 = analyzeStmt(firstPred, unionedRecord);
			for (Unit predStmt : preds) {
				result2.intersect(analyzeStmt(predStmt, unionedRecord));
			}
			result.merge(result1);
			result.merge(result2);
			Map<String, Object> mod = new HashMap<String, Object>();
			mod.put("type", "merge");
			mod.put("record1", outRecord.toString());
			mod.put("record2", unionedRecord.toString());
			mod.put("input1", inRecordDefs.get(outRecord));
			mod.put("input2", inRecordDefs.get(unionedRecord));
			modifications.put(stmt, mod);
			analyzed.get(stmt).put(outRecord, result);
			return result;
		}

		Unit firstPred = preds.get(0);
		preds.remove(0);
		result.merge(analyzeStmt(firstPred, outRecord));
		for (Unit predStmt : preds) {
			result.intersect(analyzeStmt(predStmt, outRecord));
		}

		// check whether it is a setFieldCall
		InstanceInvokeExpr setFieldCall = findInvokeExpr(stmt, SET_FIELD);
		if (setFieldCall != null && setFieldCall.getBase().equals(outRecord)) {
			int attributePosition = getAttributePosition(setFieldCall, stmt);
			if (isSetNull(setFieldCall)) {
				for (Integer origin : result.origin) {
					result.projection.put(attributePosition, origin);
					Map<String, Object> mod = new HashMap<String, Object>();
					mod.put("type", "project");
					mod.put("attribute", attributePosition);
					mod.put("record", outRecord.toString());
					modifications.put(stmt, mod);
					analyzed.get(stmt).put(outRecord, result);
				}
			} else {
				Local setValue = getSetValue(setFieldCall);

				if (!(setValue instanceof Local)) {
					result.write.add(attributePosition);
					Map<String, Object> mod = new HashMap<String, Object>();
					mod.put("type", "write");
					mod.put("attribute", attributePosition);
					mod.put("record", outRecord.toString());
					modifications.put(stmt, mod);
				} else {
					List<Unit> setValueDefs = useDef.getDefsOfAt(setValue, stmt);
					// only regard "left" and "right" input here, not yet n-ary
					if (allFromGetStmts(setValueDefs, attributePosition, 0, useDef)) {
						result.copy.put(attributePosition, 0);
						Map<String, Object> mod = new HashMap<String, Object>();
						mod.put("type", "copy");
						mod.put("attribute", attributePosition);
						mod.put("record", outRecord.toString());
						mod.put("input", 0);
						modifications.put(stmt, mod);
					} else if (allFromGetStmts(setValueDefs, attributePosition, 1, useDef)) {
						result.copy.put(attributePosition, 1);
						Map<String, Object> mod = new HashMap<String, Object>();
						mod.put("type", "copy");
						mod.put("attribute", attributePosition);
						mod.put("record", outRecord.toString());
						mod.put("input", 1);
						modifications.put(stmt, mod);
					} else {
						result.write.add(attributePosition);
						Map<String, Object> mod = new HashMap<String, Object>();
						mod.put("type", "write");
						mod.put("attribute", attributePosition);
						mod.put("record", outRecord.toString());
						modifications.put(stmt, mod);
					}
				}
			}

		}
		InstanceInvokeExpr setNullCall = findInvokeExpr(stmt, SET_NULL);
		if (setNullCall != null && setNullCall.getBase().equals(outRecord)) {
			for (Integer origin : result.origin) {
				result.projection.put(getAttributePosition(setNullCall, stmt), origin);
			}
		}
		analyzed.get(stmt).put(outRecord, result);
		return result;
	}

	/**
	 * Gets only the "real" predecessors of a statements, not those that are
	 * also successors (as one might encounter in a loop). Also using the
	 * successors would lead to an infinite loop in the recursive algorithm.
	 */
	private List<Unit> getRealPreds(Unit stmt) {
		List<Unit> preds = new LinkedList<Unit>(unitGraph.getPredsOf(stmt));
		List<Unit> realPreds = new LinkedList<Unit>();

		// little workaround for how java compiles
		// for loops with iterators ( for( ... : ...))
		// using a goto
		Unit directPred = units.getPredOf(stmt);
		if (directPred instanceof GotoStmt) {
			preds.addAll(unitGraph.getPredsOf(directPred));
		}

		for (Unit pred : preds) {
			if (!units.follows(pred, stmt)) {
				realPreds.add(pred);
			}
		}
		return realPreds;
	}

	/**
	 * Recursively checks whether the setValueDefs are all in the getStmts of
	 * the correct attribute by following units where a local variable is
	 * assigned to another local or assigned via a cast.
	 */
	private boolean allFromGetStmts(List<Unit> setValueDefs, int attributePosition, int input, LocalDefs useDef) {
		for (Unit setValueDef : setValueDefs) {
			if (getStmts.containsKey(setValueDef) && getStmts.get(setValueDef) == input) {
				InstanceInvokeExpr getFieldCall = findInvokeExpr(setValueDef, GET_FIELD);
				if (getFieldCall == null) {
					return false;
				}
				if (attributePosition != getAttributePosition(getFieldCall, setValueDef)) {
					return false;
				}
				continue;
			} else if (setValueDef instanceof AssignStmt) {
				AssignStmt assign = (AssignStmt) setValueDef;
				Value rhs = assign.getRightOp();
				if (rhs instanceof CastExpr) {
					CastExpr cast = (CastExpr) rhs;
					rhs = cast.getOp();
				}
				if (!(rhs instanceof Local)) {
					return false;
				}
				Local rhsLocal = (Local) rhs;
				List<Unit> recSetValueDefs = useDef.getDefsOfAt(rhsLocal, setValueDef);
				if (recSetValueDefs.size() <= 0) {
					return false;
				}
				if (!allFromGetStmts(recSetValueDefs, attributePosition, input, useDef)) {
					return false;
				}
			} else {
				return false;
			}
		}
		return true;
	}

	/**
	 * Returns the position of the attribute of a getField call. This only works
	 * when the attribute position is given as an integer constant.
	 * 
	 * Returns -1 if the position could not be retrieved.
	 */
	private int getAttributePosition(InstanceInvokeExpr getSetFieldCall, Unit unit) {
		Value value = getSetFieldCall.getArg(0);
		if (value instanceof IntConstant) {
			IntConstant position = (IntConstant) value;
			return position.value;
		}
		if (value instanceof Local) {
			Local local = (Local) value;
			List<Unit> defs = useDef.getDefsOfAt(local, unit);
			if (defs.size() == 1) {
				Unit def = defs.get(0);
				if (def instanceof AssignStmt) {
					AssignStmt assign = (AssignStmt) def;
					if (assign.getRightOp() instanceof IntConstant) {
						IntConstant position = (IntConstant) assign.getRightOp();
						return position.value;
					}
				}
			}
		}
		return -1;
	}

	/**
	 * Returns the Value that is stored to a field in a setField call.
	 */
	private Local getSetValue(InstanceInvokeExpr setFieldCall) {
		Value value = setFieldCall.getArg(1);
		if (value instanceof Local) {
			return (Local) value;
		}
		return null;
	}

	/**
	 * Returns the Value that is unioned to a record.
	 */
	private Local getUnionedValue(InstanceInvokeExpr unionCall) {
		Value value = unionCall.getArg(0);
		if (value instanceof Local) {
			return (Local) value;
		}
		return null;
	}

	/**
	 * Returns true when the given invoke expr is a setField call that sets the
	 * attribute to null.
	 */
	private boolean isSetNull(InstanceInvokeExpr setFieldCall) {
		Value value = setFieldCall.getArg(1);
		if (value instanceof NullConstant) {
			return true;
		}
		return false;
	}

	/**
	 * Returns the actual InstanceInvokeExpr for the method call to the given
	 * method in the given unit. This is necessary because the
	 * InstanceInvokeExpr might be "buried" in an AssignStmt or a InvokeStmt.
	 * 
	 * Returns null when no matching InvokeExpr can be found.
	 */
	private InstanceInvokeExpr findInvokeExpr(Unit unit, String methodName) {
		for (ValueBox vBox : unit.getUseBoxes()) {
			Value value = vBox.getValue();
			if (value instanceof InstanceInvokeExpr) {
				InstanceInvokeExpr invoke = (InstanceInvokeExpr) value;
				if (invoke.getMethod().getName().equals(methodName)) {
					return invoke;
				}
			}
		}
		return null;
	}

	public int getLowerBound() {
		return lowerBound;
	}

	public int getUpperBound() {
		return upperBound;
	}

	public FieldSet getWriteSet() {
		return writeSet;
	}

	public FieldSet getLeftReadSet() {
		return leftReadSet;
	}

	public FieldSet getLeftExplicitProjectionSet() {
		return leftExplicitProjectionSet;
	}

	public FieldSet getLeftExplicitCopySet() {
		return leftExplicitCopySet;
	}

	public ImplicitOperation.ImplicitOperationMode getLeftImplicitOperation() {
		return leftImplOp;
	}

	public FieldSet getRightReadSet() {
		return rightReadSet;
	}

	public FieldSet getRightExplicitProjectionSet() {
		return rightExplicitProjectionSet;
	}

	public FieldSet getRightExplicitCopySet() {
		return rightExplicitCopySet;
	}

	public ImplicitOperation.ImplicitOperationMode getRightImplicitOperation() {
		return rightImplOp;
	}

	public String getUDFBodyAsJSON() throws JsonGenerationException, JsonMappingException, IOException {
		Map<String, Object> result = new HashMap<String, Object>();

		result.put("read", readSet);

		List<Object> jsonStatements = new LinkedList<Object>();

		for (Unit u : units) {
			Map<String, Object> jsonStatement = new HashMap<String, Object>();
			jsonStatement.put("stmt", u.toString());
			Map<String, Object> sets = new HashMap<String, Object>();
			if (analyzed.containsKey(u)) {
				for (Map.Entry<Local, AttributeSets> e : analyzed.get(u).entrySet()) {
					sets.put(e.getKey().toString(), e.getValue().toMap());
				}
			}
			jsonStatement.put("sets", sets);

			Set<Integer> predsMap = new HashSet<Integer>();
			List<Unit> preds = getRealPreds(u);
			for (Unit p : preds) {
				int index = indexOfUnitInChain(units, p);
				if (index != -1) {
					predsMap.add(index);
				}
			}
			jsonStatement.put("preds", predsMap);
			if (modifications.containsKey(u)) {
				jsonStatement.put("modification", modifications.get(u));
			}
			jsonStatements.add(jsonStatement);
		}

		result.put("stmts", jsonStatements);

		result.put("emits", emits);

		ObjectMapper mapper = new ObjectMapper();
		return mapper.defaultPrettyPrintingWriter().writeValueAsString(result);
	}

	/**
	 * Horribly inefficient helper for determining the index of statements in
	 * JSON generation.
	 */
	private int indexOfUnitInChain(Chain<Unit> chain, Unit u) {
		int index = -1;
		int i = 0;
		for (Unit o : chain) {
			if (o.equals(u)) {
				index = i;
				break;
			}
			++i;
		}
		return index;
	}

	/**
	 * Creates the "old-style" attribute sets from the new sets determined by
	 * the recursive algorithm.
	 */
	private void createLegacySets(AttributeSets result) {
		leftReadSet = new FieldSet();
		rightReadSet = new FieldSet();
		for (Map.Entry<Integer, Integer> att : readSet.entrySet()) {
			if (att.getValue() == 0) {
				leftReadSet.add(att.getKey());
			}
			if (att.getValue() == 1) {
				rightReadSet.add(att.getKey());
			}
		}

		writeSet = new FieldSet(result.write);

		if (result.origin.contains(0)) {
			leftImplOp = ImplicitOperationMode.Copy;
			leftExplicitCopySet = new FieldSet();
			leftExplicitProjectionSet = new FieldSet();
			for (Map.Entry<Integer, Integer> att : result.projection.entrySet()) {
				if (att.getValue() == 0) {
					leftExplicitProjectionSet.add(att.getKey());
				}
			}
		} else {
			leftImplOp = ImplicitOperationMode.Projection;
			leftExplicitCopySet = new FieldSet(result.copy.keySet());
			leftExplicitProjectionSet = new FieldSet();
		}
		if (result.origin.contains(1)) {
			rightImplOp = ImplicitOperationMode.Copy;
			rightExplicitCopySet = new FieldSet();
			rightExplicitProjectionSet = new FieldSet();
			for (Map.Entry<Integer, Integer> att : result.projection.entrySet()) {
				if (att.getValue() == 1) {
					rightExplicitProjectionSet.add(att.getKey());
				}
			}
		} else {
			rightImplOp = ImplicitOperationMode.Projection;
			rightExplicitCopySet = new FieldSet(result.copy.keySet());
			rightExplicitProjectionSet = new FieldSet();
		}
	}

	/**
	 * For keeping all the attribute sets, also provides the MERGE and INTERSECT
	 * methods required by the algorithm.
	 * 
	 * @author Aljoscha Krettek
	 */
	private static class AttributeSets {
		Set<Integer> origin = new HashSet<Integer>();
		Set<Integer> write = new HashSet<Integer>();
		Map<Integer, Integer> copy = new HashMap<Integer, Integer>();
		Map<Integer, Integer> projection = new HashMap<Integer, Integer>();

		public void intersect(AttributeSets other) {
			origin.retainAll(other.origin);
			write.addAll(other.write);
			projection.putAll(other.projection);
			Map<Integer, Integer> newCopy = new HashMap<Integer, Integer>();

			for (Map.Entry<Integer, Integer> att : copy.entrySet()) {
				if (other.origin.contains(att.getValue())) {
					newCopy.put(att.getKey(), att.getValue());
				}
				if (other.copy.containsKey(att.getKey()) && other.copy.get(att.getKey()) == att.getValue()) {
					newCopy.put(att.getKey(), att.getValue());
				}
			}
			for (Map.Entry<Integer, Integer> att : other.copy.entrySet()) {
				if (origin.contains(att.getValue())) {
					newCopy.put(att.getKey(), att.getValue());
				}
				if (copy.containsKey(att.getKey()) && copy.get(att.getKey()) == att.getValue()) {
					newCopy.put(att.getKey(), att.getValue());
				}
			}
			copy = newCopy;
		}

		public void merge(AttributeSets other) {
			origin.addAll(other.origin);
			write.addAll(other.write);
			projection.putAll(other.projection);
			copy.putAll(other.copy);
		}

		public Map<String, Object> toMap() {
			Map<String, Object> map = new HashMap<String, Object>();
			map.put("origin", origin);
			map.put("write", write);
			map.put("copy", copy);
			map.put("projection", projection);
			return map;
		}

		@Override
		public String toString() {
			StringBuilder result = new StringBuilder();
			result.append("ORIGIN: " + origin);
			result.append("\nWRITE: " + write);
			result.append("\nCOPY: " + copy);
			result.append("\nPROJ: " + projection);
			return result.toString();
		}
	}
}
