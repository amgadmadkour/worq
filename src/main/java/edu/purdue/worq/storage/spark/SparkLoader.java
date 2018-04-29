package edu.purdue.worq.storage.spark;

import edu.purdue.worq.rdf.RDFTriple;
import edu.purdue.worq.semanticfilters.bloom.spark.SparkBloomCreator;
import edu.purdue.worq.metadata.WORQMetadata;
import edu.purdue.worq.rdf.NameSpaceHandler;
import edu.purdue.worq.rdf.RDFDataset;
import edu.purdue.worq.storage.WORQStoreCreator;
import org.apache.commons.lang3.StringUtils;
import org.apache.jena.graph.NodeFactory;
import org.apache.jena.graph.Triple;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.storage.StorageLevel;

import java.io.File;
import java.io.Serializable;
import java.util.*;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.collect_list;

/**
 * @author Amgad Madkour
 */

public class SparkLoader implements WORQStoreCreator, Serializable {

    private static final Logger LOG = Logger.getLogger(SparkLoader.class.getName());
    private static SparkSession spark;
    private WORQMetadata metadata;

    public SparkLoader(WORQMetadata metadata) {
        Logger.getLogger(LOG.getName()).setLevel(Level.INFO);
        this.metadata = metadata;
        SparkConf conf = new SparkConf()
                .setAppName("WORQLoader")
                .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
                .set("spark.kryoserializer.buffer.max.mb", "256")
                .set("spark.sql.inMemoryColumnarStorage.compressed", "true")
                .set("spark.sql.parquet.filterPushdown", "true")
                .set("spark.sql.inMemoryColumnarStorage.batchSize", "20000")
                .set("spark.network.timeout", "9000")
                .set("spark.rpc.askTimeout", "3000")
                .set("spark.rpc.lookupTimeout", "3000")
                .set("spark.core.connection.ack.wait.timeout", "3000")
                .set("spark.core.connection.auth.wait.timeout", "3000")
                .set("spark.driver.maxResultSize", "100g")
                .set("spark.sql.parquet.cacheMetadata", "true")
                .set("spark.executor.extraJavaOptions", "-XX:+UseG1GC")
                .registerKryoClasses(new Class[]{Triple.class});

        spark = SparkSession
                .builder().config(conf).getOrCreate();
        this.metadata.setSparkSession(spark);
    }

    public boolean loadFromHDFS(String datasetPath, String separator, String localdbPath, String hdfsdbPath, RDFDataset benchmarkName) {
        NameSpaceHandler nsHandler = new NameSpaceHandler(benchmarkName);
        Map<String, Object> databaseInfo;
        List<Map> vpTablesInfo;

        Dataset<RDFTriple> triples = loadTriples(datasetPath, separator, nsHandler);
        triples.persist(StorageLevel.MEMORY_ONLY());
        LOG.info("Reading the data ..");
        //triples.toDF().write().mode(SaveMode.Overwrite).parquet(hdfsdbPath + "base.parquet");

        Dataset<Row> uniquePropDS = triples.select(col("prop"));
        List<Row> uniqueProperties = uniquePropDS.distinct().collectAsList();
        long numProperties = uniqueProperties.size();
        LOG.info("Number of unique predicates : " + numProperties);

//		long numSubs = triples.select(col("sub")).distinct().count();
//		LOG.info("Number of unique subjects : " + numSubs);
//
//		long numObjs = triples.select(col("obj")).distinct().count();
//		LOG.info("Number of unique objects : " + numObjs);
//
//		long bloomFilterSize = numSubs + numObjs;
//		metadata.setBloomFilterSize(bloomFilterSize);

        long numTriples = triples.count();

        LOG.info("RDFDataset Size :" + numTriples);
//		LOG.info("Number of partitions : " + triples.javaRDD().getNumPartitions());


        databaseInfo = metadata.DBInfo();
        //Metadata information about the data currently in the database
        databaseInfo.put("numVPTables", numProperties);
        databaseInfo.put("numProperties", numProperties);
        databaseInfo.put("numWORQTables", 0);
        databaseInfo.put("numTriples", numTriples);
        databaseInfo.put("numTuples", numTriples);

        vpTablesInfo = metadata.getVpTablesList();

        File f = new File(localdbPath + "filters");
        f.mkdirs();
        SparkBloomCreator sparkBloomCreator = new SparkBloomCreator(metadata);

        int count = 0;

//		Dataset<Row> tripleWithID = triples.withColumn("id", monotonically_increasing_id());

        for (Row row : uniqueProperties) {
            count += 1;
            LOG.info("Processing (" + count + "/" + numProperties + ") : " + row.getString(0));
            String property = row.getString(0);
            Dataset<Row> result = triples.select(col("sub"), col("obj")).where(col("prop").equalTo(property));
            result.persist(StorageLevel.MEMORY_ONLY());
            long size = result.count();
            HashMap<String, Object> vpTable = new HashMap<>();
            vpTable.put("tableName", property);
            vpTable.put("numTuples", size);
            vpTable.put("ratio", (float) size / (float) triples.count());
            vpTablesInfo.add(vpTable);
            sparkBloomCreator.createSparkBloomFilter(result, property, "sub", result.select(col("sub")).distinct().count());
            sparkBloomCreator.createSparkBloomFilter(result, property, "obj", result.select(col("obj")).distinct().count());
            result.select(col("sub"), col("obj")).write().mode(SaveMode.Overwrite).parquet(hdfsdbPath + property);
            result.unpersist();
        }
        metadata.syncMetadata();
        return true;
    }


    public Dataset<RDFTriple> loadTriples(String datasetPath, String separator, NameSpaceHandler nsHandler) {
        return spark.read().textFile(datasetPath)
                .map(new MapFunction<String, RDFTriple>() {
                    @Override
                    public RDFTriple call(String value) throws Exception {
                        if (value.endsWith("."))
                            value = value.substring(0, value.length() - 2);
                        String[] parts = value.split(separator);
                        String sub = nsHandler.parse(parts[0]);
                        String prop = nsHandler.parse(parts[1]);
                        String obj = StringUtils.join(Arrays.copyOfRange(parts, 2, parts.length), " ");
                        //String obj = parts[2];

                        if (obj.startsWith("<") && obj.endsWith(">"))
                            obj = nsHandler.parse(parts[2]);
                        return new RDFTriple(sub, prop, obj);
                    }
                }, Encoders.bean(RDFTriple.class)).sortWithinPartitions(col("sub")).repartition(col("sub"));
    }

    //A More portable data structure - EXPERIMENTAL  (Spark 2.0 or higher)
    public Dataset<Row> createCompactPropertyTable(Dataset triples, String property) {
        List<StructField> fields = new ArrayList<>();
        StructField field1 = DataTypes.createStructField("sub", DataTypes.StringType, true);
        fields.add(field1);
        StructField field2 = DataTypes.createStructField(property, DataTypes.createArrayType(DataTypes.StringType), true);
        fields.add(field2);
        //Group objects for every subject into a list and represent them in one row
        StructType schema = DataTypes.createStructType(fields);
        //Nested Column support
        Dataset<Row> propertyTable = triples.select(col("subject"), col("property"), col("object"))
                .where(col("property").equalTo(property))
                .groupBy(col("subject"))
                .agg(collect_list(col("object"))).alias(property);
        return spark.createDataFrame(propertyTable.toJavaRDD(), schema);
    }

}