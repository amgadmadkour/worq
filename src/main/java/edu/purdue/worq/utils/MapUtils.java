package edu.purdue.worq.utils;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * @author Amgad Madkour - Copied from Stackoverflow
 *         http://stackoverflow.com/questions/109383/sort-a-mapkey-value-by-values-java
 */
public class MapUtils {

	public static <K, V extends Comparable<? super V>> Map<K, V> sortByValue(Map<K, V> map, String sortType) {
		//sortType - dec: decreasing, inc: increasing
		if (sortType.equals("dec"))
			return map.entrySet()
					.stream()
					.sorted(Map.Entry.comparingByValue(Collections.reverseOrder()))
					.collect(Collectors.toMap(
							Map.Entry::getKey,
							Map.Entry::getValue,
							(e1, e2) -> e1,
							LinkedHashMap::new
					));
		else //inc
			return map.entrySet()
					.stream()
					.sorted(Map.Entry.comparingByValue())
					.collect(Collectors.toMap(
							Map.Entry::getKey,
							Map.Entry::getValue,
							(e1, e2) -> e1,
							LinkedHashMap::new
					));
	}

}
