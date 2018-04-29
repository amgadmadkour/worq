package edu.purdue.worq.queryprocessing.optimization;


import edu.purdue.worq.metadata.WORQMetadata;
import edu.purdue.worq.rdf.NameSpaceHandler;
import org.apache.jena.graph.Triple;
import org.apache.jena.sparql.algebra.Op;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import java.util.*;

import static org.apache.spark.sql.functions.col;

/**
 * @author Amgad Madkour
 */
public class Planner {

	private WORQMetadata metadata;
	private static Logger LOG = Logger.getLogger(Planner.class.getName());
	private List<Map> tablesStatistics;

	public Planner(WORQMetadata metadata) {
		this.metadata = metadata;
		Logger.getLogger(LOG.getName()).setLevel(Level.DEBUG);
	}

	public Op bgpOptimizeStats(Op opRoot) {
		if (tablesStatistics == null) {
			LOG.debug("Tables Statistics == NULL, Set Table Statistics before running bgpOptimizer");
			System.exit(-1);
		}
		BGPOptimizerStats bgpOptimizer = new BGPOptimizerStats(metadata, tablesStatistics);
		Op op = bgpOptimizer.optimize(opRoot);
		return op;
	}

	public void setTablesStatistics(List<Map> tablesStatistics) {
		this.tablesStatistics = tablesStatistics;
	}

	public Dataset<Row> evaluate(ArrayList<Triple> queryTriples, HashMap<String, Dataset<Row>> dataFrames) {

		NameSpaceHandler handler = new NameSpaceHandler(this.metadata.getBenchmarkName());

		String firstPropName = handler.parse(queryTriples.get(0).getPredicate().toString());
		Dataset<Row> rootTable = dataFrames.get(firstPropName);

		Triple t1 = queryTriples.get(0);
		String tripleSub = handler.parse(t1.getSubject().toString());
		String tripleObj = handler.parse(t1.getObject().toString());
		HashSet<String> varList = new HashSet<>();

		if (t1.getSubject().isVariable()) {
			varList.add(tripleSub);
			rootTable = rootTable.select(col("sub").as(tripleSub), col("obj"));
		} else {
			rootTable = rootTable.filter(col("sub").equalTo(tripleSub));
		}

		if (t1.getObject().isVariable()) {
			varList.add(tripleObj);
			if (t1.getSubject().isVariable())
				rootTable = rootTable.select(col(tripleSub), col("obj").as(tripleObj));
			else
				rootTable = rootTable.select(col("obj").as(tripleObj));
		} else {
			rootTable = rootTable.filter(col("obj").equalTo(tripleObj));
			if (t1.getSubject().isVariable())
				rootTable = rootTable.select(col(tripleSub));
		}

		for (int i = 1; i < queryTriples.size(); i++) {
			String propName = handler.parse(queryTriples.get(i).getPredicate().toString());
			Dataset<Row> ds2 = dataFrames.get(propName);

			Triple t2 = queryTriples.get(i);
			tripleSub = handler.parse(t2.getSubject().toString());
			tripleObj = handler.parse(t2.getObject().toString());

			if (t2.getSubject().isVariable()) {
				if (t2.getObject().isVariable() && t2.getSubject().toString().equalsIgnoreCase(t2.getObject().toString())) {
					ds2 = ds2.select(col("sub").as("t1"), col("obj"));
				} else {
					ds2 = ds2.select(col("sub").as(tripleSub), col("obj"));
				}
			} else {
				ds2 = ds2.filter(col("sub").equalTo(tripleSub));
			}

			if (t2.getObject().isVariable()) {
				if (t2.getSubject().isVariable() && t2.getSubject().toString().equalsIgnoreCase(t2.getObject().toString())) {
					ds2 = ds2.select(col("t1"), col("obj").as("t2"));
				} else if (t2.getSubject().isVariable() && !t2.getSubject().toString().equalsIgnoreCase(t2.getObject().toString())) {
					ds2 = ds2.select(col(tripleSub), col("obj").as(tripleObj));
				} else {
					ds2 = ds2.select(col("obj").as(tripleObj));
				}
			} else {
				ds2 = ds2.filter(col("obj").equalTo(tripleObj));
				if (t2.getSubject().isVariable())
					ds2 = ds2.select(col(tripleSub));
			}

			if (tripleSub != null && tripleObj == null) {
				varList.add(tripleSub);
				rootTable = rootTable.withColumnRenamed(tripleSub, "t1");
				ds2 = ds2.withColumnRenamed(tripleSub, "t2");
				rootTable = rootTable.join(ds2, ds2.col("t2").equalTo(rootTable.col("t1")));
				rootTable = rootTable.drop("t2");
				rootTable = rootTable.withColumnRenamed("t1", tripleSub);
			} else if (tripleSub == null && tripleObj != null) {
				varList.add(tripleObj);
				rootTable = rootTable.withColumnRenamed(tripleObj, "t1");
				ds2 = ds2.withColumnRenamed(tripleObj, "t2");
				rootTable = rootTable.join(ds2, ds2.col("t2").equalTo(rootTable.col("t1")));
				rootTable = rootTable.drop("t2");
				rootTable = rootTable.withColumnRenamed("t1", tripleObj);
			} else if (tripleSub != null && tripleObj != null) {
				if (varList.contains(tripleSub) && varList.contains(tripleObj)) {
					if (tripleSub.equalsIgnoreCase(tripleObj)) {
						rootTable = rootTable.withColumnRenamed(tripleSub, "t3");
						rootTable = rootTable.join(ds2, ds2.col("t1").equalTo(rootTable.col("t3")).and(ds2.col("t2").equalTo(rootTable.col("t3"))));
						rootTable = rootTable.drop("t2");
						rootTable = rootTable.drop("t3");
						rootTable = rootTable.withColumnRenamed("t1", tripleSub);
					} else {
						ds2 = ds2.select(col(tripleSub).as("t2"), col(tripleObj).as("t4"));
						rootTable = rootTable.withColumnRenamed(tripleSub, "t1");
						rootTable = rootTable.withColumnRenamed(tripleObj, "t3");
						rootTable = rootTable.join(ds2, ds2.col("t2").equalTo(rootTable.col("t1")).and(ds2.col("t4").equalTo(rootTable.col("t3"))));
						rootTable = rootTable.drop("t2");
						rootTable = rootTable.drop("t4");
						rootTable = rootTable.withColumnRenamed("t1", tripleSub);
						rootTable = rootTable.withColumnRenamed("t3", tripleObj);
					}
				} else if (varList.contains(tripleSub)) {
					varList.add(tripleObj);
					rootTable = rootTable.withColumnRenamed(tripleSub, "t1");
					ds2 = ds2.withColumnRenamed(tripleSub, "t2");
					rootTable = rootTable.join(ds2, ds2.col("t2").equalTo(rootTable.col("t1")));
					rootTable = rootTable.drop("t2");
					rootTable = rootTable.withColumnRenamed("t1", tripleSub);
				} else {
					varList.add(tripleSub);
					rootTable = rootTable.withColumnRenamed(tripleObj, "t1");
					ds2 = ds2.withColumnRenamed(tripleObj, "t2");
					rootTable = rootTable.join(ds2, ds2.col("t2").equalTo(rootTable.col("t1")));
					rootTable = rootTable.drop("t2");
					rootTable = rootTable.withColumnRenamed("t1", tripleObj);
				}
			} else { //Cross-product
				rootTable = rootTable.join(ds2);
			}
		}
		return rootTable;
	}

	//			/** Reorder properties based on the join column **/
//
//			ArrayList<Triple> reorderedTriples = new ArrayList<>();
//			ArrayList<Triple> sortedTriples = new ArrayList<>();
//			int numTriples = propertyTriples.size();
//			for (String prop : sortedProperties) {
//				sortedTriples.add(propertyTriples.get(prop));
//			}
//
//			Triple firstTriple = sortedTriples.get(0);
//			HashSet<String> joinVars = new HashSet<>();
//			ArrayList<Triple> literalTriples = new ArrayList<>();
//			ArrayList<Triple> joinedTriples = new ArrayList<>();
//
//			if (firstTriple.getSubject().isVariable())
//				joinVars.add(firstTriple.getSubject().toString());
//			else
//				literalTriples.add(firstTriple);
//
//			if (firstTriple.getObject().isVariable())
//				joinVars.add(firstTriple.getObject().toString());
//			else
//				literalTriples.add(firstTriple);
//
//			reorderedTriples.add(sortedTriples.get(0));
//			sortedTriples.remove(0);
//
//			while (reorderedTriples.size() != numTriples) {
//
//				joinedTriples = new ArrayList<>();
//				literalTriples = new ArrayList<>();
//
//				for (int i = 0; i < sortedTriples.size(); i++) {
//					Triple t2 = sortedTriples.get(i);
//					boolean subJoins = false;
//					boolean objJoins = false;
//
//					if (t2.getSubject().isVariable() && joinVars.contains(t2.getSubject().toString())) {
//						subJoins = true;
//					} else if (t2.getSubject().isVariable()) {
//						joinVars.add(t2.getSubject().toString());
//					} else {
//						if (!literalTriples.contains(t2))
//							literalTriples.add(t2);
//					}
//
//					if (t2.getObject().isVariable() && joinVars.contains(t2.getObject().toString())) {
//						objJoins = true;
//					} else if (t2.getObject().isVariable()) {
//						joinVars.add(t2.getObject().toString());
//					} else {
//						if (!literalTriples.contains(t2))
//							literalTriples.add(t2);
//					}
//
//					if (subJoins || objJoins) {
//						joinedTriples.add(t2);
//					} else {
//						joinVars.remove(t2.getSubject().toString());
//						joinVars.remove(t2.getObject().toString());
//						literalTriples.remove(t2);
//					}
//				}
//
//				if (joinedTriples.size() > 0) {
//					for (Triple t : joinedTriples) {
//						if (!literalTriples.contains(t)) {
//							sortedTriples.remove(t);
//							reorderedTriples.add(t);
//						}
//					}
//					if (literalTriples.size() > 0) {
//						for (int i = 0; i < literalTriples.size(); i++) { // first one is smallest (add to begining of the list)
//							sortedTriples.remove(literalTriples.get(i));
//							boolean added = false;
//							for (int j = 0; j < reorderedTriples.size(); j++) {
//								if (reorderedTriples.get(j).getSubject().toString().equals(literalTriples.get(i).getSubject().toString()) ||
//										reorderedTriples.get(j).getSubject().toString().equals(literalTriples.get(i).getObject().toString()) ||
//										reorderedTriples.get(j).getObject().toString().equals(literalTriples.get(i).getSubject().toString()) ||
//										reorderedTriples.get(j).getObject().toString().equals(literalTriples.get(i).getObject().toString())) {
//
//									if (j == 0) {
//										reorderedTriples.add(0, literalTriples.get(i));
//										added = true;
//										break;
//									} else if ((j - 1 >= 0) && (reorderedTriples.get(j - 1).getSubject().equals(literalTriples.get(i).getSubject())
//											|| reorderedTriples.get(j - 1).getSubject().equals(literalTriples.get(i).getObject())
//											|| reorderedTriples.get(j - 1).getObject().equals(literalTriples.get(i).getSubject())
//											|| reorderedTriples.get(j - 1).getObject().equals(literalTriples.get(i).getObject()))) {
//										reorderedTriples.add(j, literalTriples.get(i));
//										added = true;
//										break;
//									}
//								}
//							}
//							if (!added)
//								reorderedTriples.add(literalTriples.get(i));
//						}
//					}
//				} else {
//					while (sortedTriples.size() > 0) {
//						reorderedTriples.add(sortedTriples.get(0));
//						sortedTriples.remove(0);
//					}
//				}
//			}
//
//			LOG.debug("\tAFTER Reordering:");
//			reorderedTriples.forEach(LOG::debug);
//
//			/** End of Reordering **/



}
