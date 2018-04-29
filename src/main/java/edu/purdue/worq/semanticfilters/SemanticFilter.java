package edu.purdue.worq.semanticfilters;

import edu.purdue.worq.metadata.WORQMetadata;
import edu.purdue.worq.queryprocessing.optimization.CardinalityEstimation;
import edu.purdue.worq.utils.Permute;
import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import orestes.bloomfilter.BloomFilter;
//import org.apache.spark.util.sketch.BloomFilter;


import java.io.Serializable;
import java.util.*;

/**
 * @author Amgad Madkour
 */
public class SemanticFilter implements Serializable {

	private WORQMetadata metadata;
	private static Logger LOG = Logger.getLogger(SemanticFilter.class.getName());

	public SemanticFilter(WORQMetadata metadata) {
		Logger.getLogger(LOG.getName()).setLevel(Level.INFO);
		this.metadata = metadata;
	}

	public String getSemanticFilter(String queryProperty, HashMap<String, String> newPropTableNames, HashMap<String, ArrayList<String>> queryJoins) {

		//Data Preparation
		String realProperty = queryProperty.split("\\_\\d+")[0];
		ArrayList<ArrayList<String>> matchingJoins = new ArrayList<>();
		for (Map.Entry<String, ArrayList<String>> entry : queryJoins.entrySet()) {
			if (entry.getValue().contains(queryProperty + "_TRPS") || entry.getValue().contains(queryProperty + "_TRPO")) {
				ArrayList<String> jnProps = new ArrayList<>();
				for (String val : entry.getValue()) {
					jnProps.add(val.replaceAll("\\_\\d+", ""));
				}
				matchingJoins.add(jnProps);
			}
		}

		HashSet<String> candidate = new HashSet<>();
		//Get all permutations of the join in order to determine the appropriate column names
		for (ArrayList<String> entry : matchingJoins) {
			HashSet<String> freqJoins = new HashSet<>();
			List<List<String>> allcombine = new ArrayList<>();
			for (int i = 0; i < entry.size(); i++) {
				for (int j = i + 1; j < entry.size(); j++) {
					List<String> combinations = new ArrayList<>();
					combinations.add(entry.get(i));
					combinations.add(entry.get(j));
					allcombine.add(combinations);
				}
			}
			if (entry.size() > 2)
				allcombine.add(entry);
			for (List<String> candid : allcombine) {
				Permute perm = new Permute();
				List<List<String>> joinColumns = perm.permute(candid, 0);
				for (List<String> combination : joinColumns) {
					if (combination.get(0).startsWith(realProperty))
						freqJoins.add(StringUtils.join(combination, "_JOIN_"));
				}
			}

			for (String joinName : freqJoins) {
				String[] joinCond = joinName.split("\\_JOIN\\_");
				ArrayList<String> tableJoins = new ArrayList<>();
				for (String p : joinCond) {
					tableJoins.add(p.split("\\_TRP")[0]);
				}
				Map mp = metadata.getWORQTableInfo(newPropTableNames.get(realProperty));
				String schema = (String) mp.get("partitionProperties");
				String[] subTables = schema.split(",");
				int matchingEntries = 0;
				for (String jn : tableJoins) {
					if (Arrays.asList(subTables).contains(jn)) {
						matchingEntries += 1;
					}
				}
				if (matchingEntries <= tableJoins.size()) {
					candidate.add(joinName);
				}
			}
		}
		CardinalityEstimation estimator = new CardinalityEstimation(metadata);

		return estimator.simpleEstimation(newPropTableNames.get(realProperty), candidate);
	}

	public HashMap<String, String> identifySemanticFilters(String queryProperty, HashMap<String, ArrayList<String>> queryJoins) {

		HashMap<String, String> filters = new HashMap<>();
		HashSet<String> subFilters = new HashSet<>();
		HashSet<String> objFilters = new HashSet<>();

		for (Map.Entry<String, ArrayList<String>> entry : queryJoins.entrySet()) {
			//Add all partitions that join with the current property on its subject column
			if (entry.getValue().contains(queryProperty + "_TRPS")) {
				subFilters.addAll(entry.getValue());
			}
			//Add all partitions that join with the current property on its object column
			if (entry.getValue().contains(queryProperty + "_TRPO")) {
				objFilters.addAll(entry.getValue());
			}
		}

		subFilters.remove(queryProperty + "_TRPS");
		objFilters.remove(queryProperty + "_TRPO");

		if (subFilters.size() > 0) {
			subFilters.remove(queryProperty + "_TRPS");
			ArrayList subFiltersList = new ArrayList(subFilters);
			Collections.sort(subFiltersList);
			String subjectFilterName = queryProperty + "_TRPS" + "_JOIN_" + StringUtils.join(subFiltersList, "_JOIN_");
			filters.put("sub", subjectFilterName);
		}

		if (objFilters.size() > 0) {
			objFilters.remove(queryProperty + "_TRPO");
			ArrayList objFiltersList = new ArrayList(objFilters);
			Collections.sort(objFiltersList);
			String objectFilterName = queryProperty + "_TRPO" + "_JOIN_" + StringUtils.join(objFiltersList, "_JOIN_");
			filters.put("obj", objectFilterName);
		}

		return filters;
	}


	public ArrayList<BloomFilter> getBloomFilters(String exceptPropertyName, String column, String semanticFilter) {
		String trpTag;

		if (column.equals("sub"))
			trpTag = "_TRPS";
		else
			trpTag = "_TRPO";

		LOG.debug("Semantic Filter : " + semanticFilter);
		List<String> filters = new LinkedList<String>(Arrays.asList(semanticFilter.split("\\_JOIN\\_")));
		LOG.debug("Filters before : ");
		filters.forEach(LOG::debug);
		LOG.debug("Except : " + exceptPropertyName + trpTag);
		filters.remove(exceptPropertyName + trpTag);
		filters.forEach(LOG::debug);

		ArrayList<BloomFilter> all = new ArrayList<>();
		for (String bfName : filters) {
			//Determine the column name of matching partitions that join with the current partition/property
			String[] parts = bfName.split("\\_TRP");
			String name = parts[0].split("\\_\\d+")[0];
			String col = parts[1];
			//determine the column of the other property and add to appropriate list
			if (col.equalsIgnoreCase("S")) {
				//LOG.debug("SUB - Applying "+bfName);
				all.add(this.metadata.getBroadcastFilters().value().get(name).get("sub"));
			} else {
				//LOG.debug("SUB - Applying "+bfName);
				all.add(this.metadata.getBroadcastFilters().value().get(name).get("obj"));
			}
		}
		return all;
	}

	public JavaRDD<Row> compute(String queryProperty, String column, String semanticFilter, Dataset<Row> current) {

		ArrayList<BloomFilter> all = getBloomFilters(queryProperty, column, semanticFilter);

		JavaRDD<Row> matches = current.toJavaRDD().filter(row -> {
			//Apply the filters on the subject field
			for (int i = 0; i < all.size(); i++) {
				if (!all.get(i).contains((String) row.getAs(column))) {
					return false;
				}
			}
			return true;
		});
		return matches;
	}

	public Dataset<Row> computeDataset(String queryProperty, String column, String semanticFilter, Dataset<Row> current) {

		ArrayList<BloomFilter> all = getBloomFilters(queryProperty, column, semanticFilter);

		return current.filter((FilterFunction<Row>) value -> {
			//Apply the filters on the subject field
			for (int i = 0; i < all.size(); i++) {
				if (!all.get(i).contains(value.getAs(column).toString()))
					return false;
			}
			return true;
		});
	}

    public Dataset<Row> computeBoth(String queryProperty, HashMap<String, String> semanticFilters, Dataset<Row> current) {

        final ArrayList<BloomFilter> allSub;
        final ArrayList<BloomFilter> allObj;

        if (semanticFilters.containsKey("sub"))
            allSub = getBloomFilters(queryProperty, "sub", semanticFilters.get("sub"));
        else
            allSub = new ArrayList<>();

        if (semanticFilters.containsKey("obj"))
            allObj = getBloomFilters(queryProperty, "obj", semanticFilters.get("obj"));
        else
            allObj = new ArrayList<>();

        return current.filter((FilterFunction<Row>) row -> {
            //Apply the filters on the subject field
            for (int i = 0; i < allSub.size(); i++) {
                if (!allSub.get(i).contains(row.getAs("sub").toString())) {
                    return false;
                }
            }
            //Apply the filters on the object field
            for (int i = 0; i < allObj.size(); i++) {
                if (!allObj.get(i).contains(row.getAs("obj").toString())) {
                    return false;
                }
            }
            return true;
        });
    }


	public JavaRDD<Row> computeAll(String queryProperty, HashMap<String, String> semanticFilters, Dataset<Row> current) {

		final ArrayList<BloomFilter> allSub;
		final ArrayList<BloomFilter> allObj;

		if (semanticFilters.containsKey("sub"))
			allSub = getBloomFilters(queryProperty, "sub", semanticFilters.get("sub"));
		else
			allSub = new ArrayList<>();

		if (semanticFilters.containsKey("obj"))
			allObj = getBloomFilters(queryProperty, "obj", semanticFilters.get("obj"));
		else
			allObj = new ArrayList<>();

		return current.toJavaRDD().filter(row -> {
			//Apply the filters on the subject field
			for (int i = 0; i < allSub.size(); i++) {
				if (!allSub.get(i).contains(row.getAs("sub").toString())) {
					return false;
				}
			}
			//Apply the filters on the object field
			for (int i = 0; i < allObj.size(); i++) {
				if (!allObj.get(i).contains(row.getAs("obj").toString())) {
					return false;
				}
			}
			return true;
		});
	}

	public Dataset<Row> computeAllDataset(String queryProperty, HashMap<String, String> semanticFilters, Dataset<Row> current) {

		final ArrayList<BloomFilter> allSub;
		final ArrayList<BloomFilter> allObj;

		if (semanticFilters.containsKey("sub"))
			allSub = getBloomFilters(queryProperty, "sub", semanticFilters.get("sub"));
		else
			allSub = new ArrayList<>();

		if (semanticFilters.containsKey("obj"))
			allObj = getBloomFilters(queryProperty, "obj", semanticFilters.get("obj"));
		else
			allObj = new ArrayList<>();

		return current.filter((FilterFunction<Row>) row -> {
			//Apply the filters on the subject field
			for (int i = 0; i < allSub.size(); i++) {
				if (!allSub.get(i).contains(row.getAs("sub").toString())) {
					return false;
				}
			}
			//Apply the filters on the object field
			for (int i = 0; i < allObj.size(); i++) {
				if (!allObj.get(i).contains(row.getAs("obj").toString())) {
					return false;
				}
			}
			return true;
		});
	}



//	public void destroyBroadcastVariables() {
//		for (int i = 0; i < broadcastVariables.size(); i++) {
//			broadcastVariables.get(i).destroy();
//		}
//	}
}
