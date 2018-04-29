package edu.purdue.worq;

import edu.purdue.worq.metadata.WORQMetadata;
import edu.purdue.worq.queryprocessing.execution.ExecutionEngine;
import edu.purdue.worq.queryprocessing.execution.WORQExecutionEngine;
import edu.purdue.worq.queryprocessing.execution.WORQResult;
import edu.purdue.worq.queryprocessing.execution.spark.*;
import edu.purdue.worq.queryprocessing.parsing.Parser;
import edu.purdue.worq.utils.CliParser;
import org.apache.jena.sparql.algebra.Op;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;

/**
 * @author Amgad Madkour
 */
public class WORQQuery {

	private static Logger LOG = Logger.getLogger(WORQQuery.class.getName());
	private static WORQMetadata metadata;

	private WORQExecutionEngine engine;
	private Parser parser;

	public WORQQuery(String[] args) {
		Logger.getLogger(LOG.getName()).setLevel(Level.DEBUG);
		metadata = new WORQMetadata();
		new CliParser(args).parseQueryParams(metadata);
		metadata.loadMetaData();

		this.parser = new Parser(metadata);

		//Initialize the execution engine
		if (metadata.getEngine() == ExecutionEngine.SPARKAPI) {
			engine = new SparkExecutionEngine(metadata);
		} else {
            engine = new RDFTableEngine(metadata);
		}
		metadata.broadcastFilters();
	}

	public WORQResult executeQuery(String query) {
		Op opRoot;

		//Parsing
		opRoot = parser.parseByID(query);

		//Planning & Evaluation & Execution
		return engine.execute(opRoot);
	}

	public int executeQueries(String queriesPath) {
		int numQueries = 0;
		try {
			PrintWriter printer = new PrintWriter(new FileWriter(metadata.getLocalDbPath() + "results-worq.txt"));
			File folder = new File(queriesPath);
			File[] listOfFiles = folder.listFiles();
			ArrayList<String> queries = new ArrayList<>();
			Arrays.sort(listOfFiles);

			for (int i = 0; i < listOfFiles.length; i++) {
				if (listOfFiles[i].isFile()) {
					queries.add(listOfFiles[i].getName());
				}
			}

			for (String qryFile : queries) {
				numQueries += 1;
                String qryName = qryFile.split("\\.")[0];
                String qry = new String(Files.readAllBytes(Paths.get(metadata.getQueryFilePath() + qryFile)));
				metadata.setCurrentQueryName(qryName);
				WORQResult r = executeQuery(qry);
				printer.println(qryName + "\t" + r.getNumResults() + "\t" + r.getTime() + "\t" + r.getTableSizes() + "\t" + r.getReductionSizes() + "\t" + r.getMaxNumJoins() + "\t" + r.isWarm() + "\t" + r.getNumTriples());
				LOG.info(numQueries + "/" + queries.size() + "\tQuery Name: " + qryName + "\tTime: " + r.getTime() + "ms (" + r.getNumResults() + ")" + "\t" + r.getReductionSizes() + "/" + r.getTableSizes() + "\t" + r.getMaxNumJoins() + "\t" + r.isWarm() + "\t" + r.getNumTriples());
			}
			printer.close();

		} catch (IOException exp) {
			exp.printStackTrace();
		}
		return numQueries;
	}

	public int executeBatchQueries(String queriesPath) {
		int numQueries = 0;
		try {
			PrintWriter printer = new PrintWriter(new FileWriter(metadata.getLocalDbPath() + "results-worq.txt"));
			File file = new File(queriesPath);
			BufferedReader rdr = new BufferedReader(new FileReader(file));
			String qry;

			while ((qry = rdr.readLine()) != null) {
				numQueries += 1;
				WORQResult r = executeQuery(qry);
				printer.println("batch-" + numQueries + "\t" + r.getNumResults() + "\t" + r.getTime() + "\t" + r.getTableSizes() + "\t" + r.getReductionSizes() + "\t" + r.getMaxNumJoins() + "\t" + r.isWarm() + "\t" + r.getNumTriples());
				LOG.info("\tQuery Name: " + "batch-" + numQueries + "\tTime: " + r.getTime() + "ms (" + r.getNumResults() + ")" + "\t" + r.getReductionSizes() + "/" + r.getTableSizes() + "\t" + r.getMaxNumJoins() + "\t" + r.isWarm() + "\t" + r.getNumTriples());
			}
			printer.close();

		} catch (IOException exp) {
			exp.printStackTrace();
		}
		return numQueries;
	}

	public void close() {
		engine.close();
	}

	public static void main(String[] args) {
		WORQQuery queryEngine = new WORQQuery(args);
		double start = System.currentTimeMillis();
		int numQueries = 0;
		if (metadata.isQueryBatch()) {
			numQueries = queryEngine.executeBatchQueries(metadata.getQueryFilePath());
		} else {
			numQueries = queryEngine.executeQueries(metadata.getQueryFilePath());
		}
		if (numQueries > 0)
			LOG.info("Executed " + numQueries + " queries");
		else
			LOG.fatal("Error executing queries");
		double time = System.currentTimeMillis() - start;
		LOG.info("Executed in " + time / 1000 + " seconds");
		queryEngine.close();
	}

}
