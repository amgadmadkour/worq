package edu.purdue.worq.queryprocessing.parsing;

import edu.purdue.worq.metadata.WORQMetadata;
import edu.purdue.worq.rdf.BGPVisitorByID;
import edu.purdue.worq.rdf.NameSpaceHandler;
import edu.purdue.worq.queryprocessing.translation.SPARQLQueryModifier;
import org.apache.jena.query.Query;
import org.apache.jena.query.QueryFactory;
import org.apache.jena.sparql.algebra.Algebra;
import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.algebra.OpWalker;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import java.util.HashMap;
import java.util.Map;

/**
 * @author Amgad Madkour
 */
public class Parser {

	private WORQMetadata metadata;
	private static Logger LOG = Logger.getLogger(Parser.class.getName());

	public Parser(WORQMetadata metadata) {
		Logger.getLogger(LOG.getName()).setLevel(Level.DEBUG);
		this.metadata = metadata;
		Logger.getLogger("org.apache.jena.arq.info").setLevel(Level.OFF);
		Logger.getLogger("org.apache.jena.arq.exec").setLevel(Level.OFF);
	}

	public Op parseByID(String queryString) {
		Query query;
		try {
			if (queryString.contains("BASE ")) {
				SPARQLQueryModifier sqm = new SPARQLQueryModifier(new NameSpaceHandler(metadata.getBenchmarkName()));
				//TODO Unit Test for convertBasePrefix
				queryString = sqm.convertBasePrefix(queryString);
			}
		} catch (Exception exp) {
			exp.printStackTrace();
			return null;
		}

		query = QueryFactory.create(queryString);

		Op opRoot = Algebra.compile(query);
		BGPVisitorByID visitor = new BGPVisitorByID(metadata.getBenchmarkName());
		OpWalker.walk(opRoot, visitor);

		//TODO Unit Test for getBgpReplacements
		HashMap<String, String> bgpReplacements = visitor.getBgpReplacements();

		queryString = queryString.replaceAll("(\\t| )+", " ");
		for (Map.Entry<String, String> entry : bgpReplacements.entrySet()) {
			queryString = queryString.replace(entry.getKey(), entry.getValue());
		}

//		LOG.debug("\t==SPARQL:\n" + queryString);
//		LOG.debug("\t==BGP Replacements :");
//		bgpReplacements.entrySet().forEach(LOG::debug);

		//Modified triples with ID
		query = QueryFactory.create(queryString);
		opRoot = Algebra.compile(query);

		return opRoot;
	}

	public Op parse(String queryString) {
		Query query;
		try {
			if (queryString.contains("BASE ")) {
				SPARQLQueryModifier sqm = new SPARQLQueryModifier(new NameSpaceHandler(metadata.getBenchmarkName()));
				//TODO Unit Test for convertBasePrefix
				queryString = sqm.convertBasePrefix(queryString);
			}
		} catch (Exception exp) {
			exp.printStackTrace();
			return null;
		}

		query = QueryFactory.create(queryString);
		Op opRoot = Algebra.compile(query);

		return opRoot;
	}


}
