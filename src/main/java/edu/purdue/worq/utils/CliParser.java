package edu.purdue.worq.utils;

import edu.purdue.worq.metadata.WORQMetadata;
import edu.purdue.worq.rdf.RDFDataset;
import edu.purdue.worq.queryprocessing.execution.ExecutionEngine;
import edu.purdue.worq.storage.ComputationType;
import orestes.bloomfilter.HashProvider.HashMethod;
import org.apache.commons.cli.*;
import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

/**
 * @author Amgad Madkour
 */
public class CliParser {

	private final Logger LOG = Logger.getLogger(CliParser.class.getName());
	private CommandLineParser parser;
	private CommandLine cmd;
	private Options options;
	private String[] args;

	public CliParser(String[] args) {
		this.parser = new BasicParser();
		this.args = args;
		this.options = new Options();

		BasicConfigurator.configure();
		Logger.getLogger(LOG.getName()).setLevel(Level.DEBUG);
	}

	private void help(String type) {
		HelpFormatter formater = new HelpFormatter();
		if (type.equals("store"))
			formater.printHelp("edu.purdue.worq.WORQLoader", options);
		else if (type.equals("execute"))
			formater.printHelp("edu.purdue.worq.WORQQuery", options);
		else if (type.equals("eval"))
			formater.printHelp("edu.purdue.worq.evaluation.EvaluatePartitions", options);
		else if (type.equals("partition"))
			formater.printHelp("edu.purdue.worq.partitioning.WORQPartitioner", options);
		else if (type.equals("bloom"))
			formater.printHelp("edu.purdue.worq.WORQBloom", options);
		System.exit(0);
	}

	private void initLoaderOptions() {
		options.addOption("l", "localdb", true, "Local database location (Contains Metadata)");
		options.addOption("r", "hdfsdb", true, "HDFS database location (Contains Parquet Files)");
		options.addOption("d", "dataset", true, "Create database for input dataset (NTriple Format Required");
		options.addOption("s", "separator", true, "Separator used between Subject/Property/Object (default is tab)");
		options.addOption("t", "computation", true, "Computation type (e.g. spark, mapreduce, centralized)");
		options.addOption("b", "benchmark", true, "Specify the benchmark/dataset name (e.g., WatDiv,LUBM or Generic)");
		options.addOption("f", "falsePositive", true, "False Positive Probability (e.g., 0.01)");
		options.addOption("h", "hashFunction", true, "Hash Function [sha384,mm3km]");
	}

	private void initExecutionOptions() {
		options.addOption("l", "localdb", true, "Local database location (Contains Metadata)");
		options.addOption("r", "hdfsdb", true, "HDFS database location (Contains Parquet Files)");
		options.addOption("q", "queryfile", true, "SPARQL Queries to execute");
        options.addOption("t", "querybatch", true, "SPARQL batch file containing queries to execute");
        options.addOption("b", "benchmark", true, "Specify the benchmark/dataset name [OPTIONS: yago, watdiv, lubm, dbpedia, generic]");
		options.addOption("e", "engine", true, "Engine to execute the query [OPTIONS: sparksql,sparkapi,ignite,sparkrdd]");
		options.addOption("c", "cores", true, "Number of cores to use for execution");
		options.addOption("s", "s2rdf", true, "S2RDF Query File");
	}

	private void initEvalOptions() {
		options.addOption("l", "localdb", true, "Local database location (Contains Metadata)");
		options.addOption("q", "queryfile", true, "SPARQL Queries to execute");
		options.addOption("i", "inputdir", true, "Input Query Directory");
		options.addOption("b", "benchmark", true, "Specify the benchmark/dataset name (e.g., WatDiv,LUBM or Generic)");
	}


	private void initPartitionOptions() {
		options.addOption("l", "localdb", true, "Local database location (Contains Metadata)");
		options.addOption("r", "hdfsdb", true, "HDFS database location (Contains Parquet Files)");
		options.addOption("q", "queryfile", true, "SPARQL Queries to execute");
		options.addOption("a", "algorithm", true, "Precomputation Algorithm");
		options.addOption("b", "benchmark", true, "Specify the benchmark/dataset name (e.g., WatDiv,LUBM or Generic)");
	}

	private void initBloomFilterOptions() {
		options.addOption("l", "localdb", true, "Local database location (Contains Metadata)");
		options.addOption("r", "hdfsdb", true, "HDFS database location (Contains Parquet Files)");
		options.addOption("t", "computation", true, "Computation type (e.g. spark, mapreduce, centralized)");
		options.addOption("f", "falsePositive", true, "False Positive Probability (e.g., 0.01)");
		options.addOption("h", "hashFunction", true, "Hash Function [sha384,mm3km]");
	}

	/**
	 * Parse the command line arguments for loading the dataset
	 *
	 * @param metadata Used for saving the argument values in the metadata object
	 */
	public void parseLoaderParams(WORQMetadata metadata) {

		initLoaderOptions();

		try {

			cmd = parser.parse(options, args);
			if (cmd.hasOption("l") &&
					cmd.hasOption("r") &&
					cmd.hasOption("d") &&
					cmd.hasOption("t") &&
					cmd.hasOption("b") &&
					cmd.hasOption("f") &&
					cmd.hasOption("h")) {
				//Validate whether the local directory exists
				Path path = Paths.get(cmd.getOptionValue("l"));
				if (Files.notExists(path)) {
					Files.createDirectory(path);
				}
				String hdfsPath = cmd.getOptionValue("r");
				String datasetPath = cmd.getOptionValue("d");
				String localDbPath = cmd.getOptionValue("l");
				String falsePositive = cmd.getOptionValue("f");
				String separator;
				Separator sep = Separator.SPACE;
				ComputationType store = ComputationType.CENTRALIZED;
				HashMethod hashMethod = HashMethod.Murmur3KirschMitzenmacher;
				RDFDataset benchmark;

				if (cmd.hasOption("s")) {
					separator = cmd.getOptionValue("s");
					if (separator.equals("tab")) {
						sep = Separator.TAB;
					} else {
						sep = Separator.SPACE;
					}
				}

				String st = cmd.getOptionValue("t");
				if (st.equalsIgnoreCase("spark")) {
					store = ComputationType.SPARK;
				}

				String bm = cmd.getOptionValue("b");
				if (bm.equalsIgnoreCase("watdiv")) {
					benchmark = RDFDataset.WATDIV;
				} else if (bm.equalsIgnoreCase("dbpedia")) {
					benchmark = RDFDataset.DBPEDIA;
				} else if (bm.equalsIgnoreCase("yago")) {
					benchmark = RDFDataset.YAGO;
				} else if (bm.equalsIgnoreCase("lubm")) {
					benchmark = RDFDataset.LUBM;
				} else {
					benchmark = RDFDataset.GENERIC;
				}

				String hf = cmd.getOptionValue("h");
				if (hf.equalsIgnoreCase("sha384")) {
					hashMethod = HashMethod.SHA384;
				}
				//Set the metadata information so that we can use it as we go along
				metadata.setLocalDbPath(localDbPath);
				metadata.setHdfsDbPath(hdfsPath);
				metadata.setDatasetPath(datasetPath);
				metadata.setDatasetSeparator(sep);
				metadata.setBenchmarkName(benchmark);
				metadata.setStore(store);
				metadata.setFalsePositiveProbability(Double.parseDouble(falsePositive));
				metadata.setHashMethod(hashMethod);

			} else {
				help("store");
			}

		} catch (ParseException e) {
			help("store");
		} catch (IOException e) {
			LOG.log(Level.FATAL, e.getMessage());
			System.exit(1);
		}

	}

	public void parseQueryParams(WORQMetadata metadata) {

		initExecutionOptions();

		try {

			cmd = parser.parse(options, args);
            if (cmd.hasOption("l") && cmd.hasOption("r") && cmd.hasOption("b") && cmd.hasOption("e") && cmd.hasOption("c") && (cmd.hasOption("q") || cmd.hasOption("t"))) {
                //Validate whether the local directory exists
				Path path = Paths.get(cmd.getOptionValue("l"));
				if (Files.notExists(path)) {
					LOG.fatal("Cannot find local database - EXITING");
					System.exit(1);
				}
				String hdfsPath = cmd.getOptionValue("r");
				String localDbPath = cmd.getOptionValue("l");
				String numberOfCores = cmd.getOptionValue("c");
				RDFDataset benchmark;
				ExecutionEngine engine;
				String s2rdfFile;

                String queryFilePath;
                if (cmd.hasOption("q")) {
                    queryFilePath = cmd.getOptionValue("q");
                    metadata.setQueryFilePath(queryFilePath, false);
                } else {
                    queryFilePath = cmd.getOptionValue("t");
                    metadata.setQueryFilePath(queryFilePath, true);
                }

				String bm = cmd.getOptionValue("b");
				if (bm.equalsIgnoreCase("watdiv")) {
					benchmark = RDFDataset.WATDIV;
				} else if (bm.equalsIgnoreCase("dbpedia")) {
					benchmark = RDFDataset.DBPEDIA;
				} else if (bm.equalsIgnoreCase("yago")) {
					benchmark = RDFDataset.YAGO;
				} else if (bm.equalsIgnoreCase("lubm")) {
					benchmark = RDFDataset.LUBM;
				} else {
					benchmark = RDFDataset.GENERIC;
				}

				String eng = cmd.getOptionValue("e");
				if (eng.equalsIgnoreCase("sparkapi")) {
					engine = ExecutionEngine.SPARKAPI;
				} else if (eng.equalsIgnoreCase("triples")) {
					engine = ExecutionEngine.TRIPLES;
				} else {
					engine = ExecutionEngine.LOCAL;
				}

				if (cmd.hasOption("s")) {
					s2rdfFile = cmd.getOptionValue("s");
					LOG.debug("S2RDF Query File: " + s2rdfFile);
				}

				//Set the metadata information so that we can use it as we go along
				metadata.setLocalDbPath(localDbPath);
				metadata.setHdfsDbPath(hdfsPath);
				metadata.setBenchmarkName(benchmark);
				metadata.setEngine(engine);
				metadata.setNumberOfCores(Integer.parseInt(numberOfCores));

			} else {
				help("execute");
			}

		} catch (ParseException e) {
			help("execute");
		}
	}

	public void parseBloomFilterParams(WORQMetadata metadata) {

		initBloomFilterOptions();

		try {

			cmd = parser.parse(options, args);
			if (cmd.hasOption("l") && cmd.hasOption("r") && cmd.hasOption("t") && cmd.hasOption("f") && cmd.hasOption("h")) {
				//Validate whether the local directory exists
				Path path = Paths.get(cmd.getOptionValue("l"));
				if (Files.notExists(path)) {
					Files.createDirectory(path);
				}
				String hdfsPath = cmd.getOptionValue("r");
				String localDbPath = cmd.getOptionValue("l");
				String falsePositiveProbability = cmd.getOptionValue("f");
				ComputationType store = ComputationType.CENTRALIZED;
				HashMethod hashMethod = HashMethod.Murmur3KirschMitzenmacher;

				String st = cmd.getOptionValue("t");
				if (st.equalsIgnoreCase("spark")) {
					store = ComputationType.SPARK;
				}

				String hf = cmd.getOptionValue("h");
				if (hf.equalsIgnoreCase("sha384")) {
					hashMethod = HashMethod.SHA384;
				}

				//Set the metadata information so that we can use it as we go along
				metadata.setLocalDbPath(localDbPath);
				metadata.setHdfsDbPath(hdfsPath);
				metadata.setStore(store);
				metadata.setHashMethod(hashMethod);
				metadata.setFalsePositiveProbability(Double.parseDouble(falsePositiveProbability));

			} else {
				help("bloom");
			}

		} catch (ParseException e) {
			help("bloom");
		} catch (IOException e) {
			LOG.log(Level.FATAL, e.getMessage());
			System.exit(1);
		}
	}
}
