package edu.purdue.worq;

import edu.purdue.worq.metadata.WORQMetadata;
import edu.purdue.worq.storage.ComputationType;
import edu.purdue.worq.storage.spark.SparkLoader;
import edu.purdue.worq.utils.CliParser;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

/**
 * @author Amgad Madkour
 */
public class WORQLoader {

	private static final Logger LOG = Logger.getLogger(WORQLoader.class.getName());

	public static void main(String[] args) {

        Logger.getLogger(LOG.getName()).setLevel(Level.INFO);

		LOG.info("Starting Loader");

		//Parse the input and aggregate the meta-data
		WORQMetadata metadata = new WORQMetadata();
		new CliParser(args).parseLoaderParams(metadata);

		LOG.debug(metadata);

		if (metadata.getStoreType() == ComputationType.SPARK) {
			LOG.info("Engine: Spark");
			double start = System.currentTimeMillis();
			SparkLoader store = new SparkLoader(metadata);
			boolean status = store.loadFromHDFS(
					metadata.getDatasetPath(),
					metadata.getDatasetSeparator(),
					metadata.getLocalDbPath(),
					metadata.getHdfsDbPath(),
					metadata.getBenchmarkName()
			);
			double time = System.currentTimeMillis() - start;
			LOG.info("Database loded in " + time / 1000 + " seconds");
			if (status == true)
				LOG.info("Success");
			else
				LOG.error("Failed to createRedisBloomFilter store");
		}
	}
}
