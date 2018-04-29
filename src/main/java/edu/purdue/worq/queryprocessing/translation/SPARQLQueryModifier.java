package edu.purdue.worq.queryprocessing.translation;

import edu.purdue.worq.rdf.NameSpaceHandler;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by Amgad Madkour
 */
public class SPARQLQueryModifier {

	private NameSpaceHandler handler;

	public SPARQLQueryModifier(NameSpaceHandler handler) {
		this.handler = handler;
	}

	public String convertBasePrefix(String query) {

		//Identify the prefix
		String resultQuery = query;
		String uri = "";
		Pattern p = Pattern.compile("BASE \\<(.+?)\\>");
		Matcher m = p.matcher(query);

		if (m.find()) {
			uri = m.group(1);
		}

		Pattern p2 = Pattern.compile("\\<(.+?)\\>");
		Matcher m2 = p2.matcher(query);

		String newBase = null;
		while (m2.find()) {
			String matchString = m2.group(1);
			if (!matchString.contains("http://")) {
				String original = "<" + matchString + ">";
				String fullUri = uri + matchString;
				String prefix = handler.parse(fullUri);
				String[] parts = prefix.split("\\_\\_");
				if (newBase == null)
					newBase = parts[0];
				prefix = prefix.replace("__", ":");
				resultQuery = resultQuery.replace(original, prefix);
			}
		}
		String baseURI = "BASE <" + uri + ">";
		String prefixURI = "PREFIX " + newBase + ": <" + uri + ">";
		resultQuery = resultQuery.replace(baseURI, prefixURI);

		return resultQuery;
	}
}
