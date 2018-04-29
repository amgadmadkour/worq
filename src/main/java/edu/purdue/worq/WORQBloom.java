package edu.purdue.worq;

import edu.purdue.worq.semanticfilters.bloom.spark.SparkBloomCreator;
import edu.purdue.worq.metadata.WORQMetadata;
import edu.purdue.worq.storage.ComputationType;
import edu.purdue.worq.utils.CliParser;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

/**
 * @author Amgad Madkour
 */
public class WORQBloom {

	private static final Logger LOG = Logger.getLogger(WORQBloom.class.getName());

	public static void main(String[] args) {

		Logger.getLogger(LOG.getName()).setLevel(Level.INFO);

		LOG.info("Starting Bloom Creator");

		//Parse the input and aggregate the meta-data
		WORQMetadata metadata = new WORQMetadata();
		new CliParser(args).parseBloomFilterParams(metadata);
		metadata.loadMetaData();

		LOG.debug(metadata);

		if (metadata.getStoreType() == ComputationType.SPARK) {
			LOG.info("Using Spark to create the Bloom Filters");

			SparkBloomCreator bloomFilterCreator = new SparkBloomCreator(metadata);

			double start = System.currentTimeMillis();
			boolean status = bloomFilterCreator.create();
			double time = System.currentTimeMillis() - start;

			LOG.info("Total Creation Time: " + time);

			if (status == true)
				LOG.info("Success");
			else
				LOG.error("Failed to createRedisBloomFilter Bloom Filters");
		}
	}
}
