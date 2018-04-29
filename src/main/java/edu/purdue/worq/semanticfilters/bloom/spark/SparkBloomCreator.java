package edu.purdue.worq.semanticfilters.bloom.spark;

import edu.purdue.worq.semanticfilters.bloom.BloomFilterCreator;
import edu.purdue.worq.metadata.WORQMetadata;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.util.sketch.BloomFilter;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.apache.spark.sql.functions.col;

/**
 * @author Amgad Madkour
 */
public class SparkBloomCreator {

	private static final Logger LOG = Logger.getLogger(SparkBloomCreator.class.getName());
	private static SparkSession spark;
	private WORQMetadata metadata;

	/***
	 * Construct filters while creating a spark context
	 * @param metadata WORQ metadata
	 */
	public SparkBloomCreator(WORQMetadata metadata) {
		Logger.getLogger(LOG.getName()).setLevel(Level.DEBUG);
		this.metadata = metadata;

		if (this.metadata.getSparkSession() == null) {
			SparkConf conf = new SparkConf()
					.setAppName("WORQBloomFilterCreator")
                    .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
                    .set("spark.kryoserializer.buffer.max.mb", "256")
                    .set("spark.sql.inMemoryColumnarStorage.compressed", "true")
                    .set("spark.sql.parquet.filterPushdown", "true")
                    .set("spark.sql.inMemoryColumnarStorage.batchSize", "20000")
                    .set("spark.network.timeout", "30000")
                    .set("spark.driver.maxResultSize", "100g")
                    .set("spark.sql.parquet.cacheMetadata", "true")
                    .set("spark.network.timeout", "10000")
                    .set("spark.rpc.askTimeout", "10000")
                    .set("spark.rpc.lookupTimeout", "10000")
                    .set("spark.core.connection.ack.wait.timeout", "10000")
                    .set("spark.core.connection.auth.wait.timeout", "10000")
                    .set("spark.executor.heartbeatInterval", "10000")
                    .set("spark.executor.extraJavaOptions", "-XX:+UseG1GC");
			spark = SparkSession
					.builder().config(conf).getOrCreate();
			this.metadata.setSparkSession(spark);
		}
	}

	public void createSparkBloomFilter(Dataset<Row> dataset, String property, String colName, long size) {
		BloomFilterCreator bloomFilterCreator = new BloomFilterCreator(metadata, size);
		String filterName = colName + "_" + property;
        bloomFilterCreator.createRedisBloomFilter(filterName, dataset.toLocalIterator());
    }

	public void createSpark2BloomFilter(Dataset<Row> dataset, String property, String colName, long size) {
		BloomFilterCreator bloomFilterCreator = new BloomFilterCreator(metadata, size);
		BloomFilter bf = dataset.stat().bloomFilter(col(colName), size, metadata.getFalsePositiveProbability());
		String filterName = colName + "_" + property;
		bloomFilterCreator.createSparkBloomFilter(bf, filterName);
	}

	public boolean create() {
		List<Map> vpList = metadata.getVpTablesList();
		ArrayList<String> uniqueProperties = new ArrayList<>();

		for (Map<String, String> entry : vpList) {
			uniqueProperties.add(entry.get("tableName"));
		}

		int count = 1;

		LOG.debug("Using Redis Bloom Filter");
		for (String property : uniqueProperties) {
			LOG.info("Processing (" + count + "/" + uniqueProperties.size() + ") : " + property);
			Dataset<Row> result = spark.read().parquet(metadata.getHdfsDbPath() + property);
            Dataset<Row> distinctSub = result.select(col("sub")).distinct();
            distinctSub.persist(StorageLevel.MEMORY_ONLY());
            createSparkBloomFilter(distinctSub, property, "sub", distinctSub.count());
            distinctSub.unpersist();
            Dataset<Row> distinctObj = result.select(col("obj")).distinct();
            distinctObj.persist(StorageLevel.MEMORY_ONLY());
            createSparkBloomFilter(distinctObj, property, "obj", distinctObj.count());
            distinctObj.unpersist();
            count += 1;
		}
		LOG.debug("Filters created successfully");
		//metadata.syncMetadata();
		return true;
	}

	public boolean createSpark2() {
		List<Map> vpList = metadata.getVpTablesList();
		ArrayList<String> uniqueProperties = new ArrayList<>();

		for (Map<String, String> entry : vpList) {
			uniqueProperties.add(entry.get("tableName"));
		}

		int count = 1;
		for (String property : uniqueProperties) {
			LOG.info("Processing (" + count + "/" + uniqueProperties.size() + ") : " + property);
			Dataset<Row> result = spark.read().parquet(metadata.getHdfsDbPath() + property);
			result.persist(StorageLevel.MEMORY_ONLY());
			long size = result.count();
			createSpark2BloomFilter(result, property, "sub", result.select(col("sub")).distinct().count());
			createSpark2BloomFilter(result, property, "obj", result.select(col("obj")).distinct().count());
			result.unpersist();
			count += 1;
		}
		LOG.debug("Filters created successfully");
		//metadata.syncMetadata();
		return true;
	}
}
