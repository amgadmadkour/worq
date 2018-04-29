package edu.purdue.worq.queryprocessing.optimization;

import edu.purdue.worq.metadata.WORQMetadata;

import java.util.HashSet;

/**
 * @author Amgad Madkour
 */
public class CardinalityEstimation {

	WORQMetadata metadata;

	public CardinalityEstimation(WORQMetadata metadata) {
		this.metadata = metadata;
	}

	public String simpleEstimation(String tableName, HashSet<String> candidate) {
		//Determine which candidate will be used to read in the data based on
		//the number of entries it contains (the smaller the better)
		//The other candidate tables are measured based on how many tuples the original table carries
		//We pick the semantic filter for a table that joins with the smallest possible table, regardless of cardinality estimation
		long minCount = Long.MAX_VALUE;
		String minCombination = null;
		for (String cand : candidate) {
			long count = metadata.getSemanticColumnCount(tableName, cand); //TODO maybe get the schema first better ?
			if (count <= 0) //Does not exist
				continue;
			if (count < minCount) {
				minCount = count;
				minCombination = cand;
			}
		}
		return minCombination;
	}
}
