package edu.purdue.worq.queryprocessing.execution.spark;

import edu.purdue.worq.metadata.WORQMetadata;
import edu.purdue.worq.rdf.BGPVisitorByID;
import edu.purdue.worq.rdf.NameSpaceHandler;
import edu.purdue.worq.queryprocessing.execution.WORQExecutionEngine;
import edu.purdue.worq.queryprocessing.execution.WORQResult;
import edu.purdue.worq.queryprocessing.optimization.Planner;
import edu.purdue.worq.semanticfilters.SemanticFilter;
import orestes.bloomfilter.BloomFilter;
import org.apache.jena.graph.NodeFactory;
import org.apache.jena.graph.Triple;
import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.algebra.OpWalker;
import org.apache.jena.sparql.algebra.op.OpBGP;
import org.apache.jena.sparql.core.BasicPattern;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.*;
import org.apache.spark.storage.StorageLevel;
import scala.Tuple3;

import java.io.Serializable;
import java.util.*;

import static org.apache.spark.sql.functions.col;

/**
 * @author Amgad Madkour
 */ 
public class SparkExecutionEngine implements Serializable, WORQExecutionEngine {

    private static Logger LOG = Logger.getLogger(SparkExecutionEngine.class.getName());
    private static SparkSession spark;
    private WORQMetadata metadata;
    private HashMap<String, Dataset<Row>> semanticFilters;
    private HashMap<String, Dataset<Row>> cachedTables;
    private HashMap<String, Long> statistics;

    public SparkExecutionEngine(WORQMetadata metadata) {
        Logger.getLogger(LOG.getName()).setLevel(Level.INFO);
        this.metadata = metadata;
        SparkConf conf = new SparkConf()
                .setAppName("SparkExecutionEngine")
                .set("spark.executor.memory", "25g")
                .set("spark.executor.extraJavaOptions", "-XX:+UseG1GC")
                .set("spark.sql.autoBroadcastJoinThreshold", "-1")
                .set("spark.sql.inMemoryColumnarStorage.batchSize", "20000")
                .set("spark.sql.shuffle.partitions", String.valueOf(metadata.getNumberOfCores()));
        spark = SparkSession
                .builder().config(conf).getOrCreate();
        this.metadata.setSparkSession(spark);
        this.semanticFilters = new HashMap<>();
        this.cachedTables = new HashMap<>();
        this.statistics = new HashMap<>();
    }

    public WORQResult execute(Op opRoot) {

        double start = System.currentTimeMillis();

        Planner planner = new Planner(metadata);

        //Initializations
        NameSpaceHandler handler = new NameSpaceHandler(this.metadata.getBenchmarkName());
        HashMap<String, Dataset<Row>> dataFrames = new HashMap<>();
        ArrayList<Triple> unboundTriples = new ArrayList<>();
        HashMap<String, String> propTableNames = new HashMap<>();
        List<Map> tablesStats = new ArrayList<>();
        boolean worqTable;
        boolean isWarm = false;
        long numResults = 0;
        int numTriples = 0;
        double cacheTime = 0;
        long reductionSizes = 0;
        long tableSizes = 0;
        int maxJoins = 0;

        //Visit BGP
        BGPVisitorByID visitor = new BGPVisitorByID(metadata.getBenchmarkName());
        OpWalker.walk(opRoot, visitor);

        //Use visited information
        ArrayList<Triple> queryTriplesWithID = visitor.getTriplesWithID();
        int lastID = visitor.getLastID() + 1;
        HashMap<String, ArrayList<String>> queryJoins = visitor.getJoinVariableEntries();

        for (Map.Entry<String, ArrayList<String>> entry : queryJoins.entrySet()) {
            int numJoins = entry.getValue().size();
            if (numJoins > maxJoins)
                maxJoins = numJoins;
        }

        LOG.debug(queryJoins);
        queryTriplesWithID.forEach(LOG::debug);

        if (visitor.getUnboundTriples().size() > 0)
            unboundTriples = visitor.getUnboundTriples();

        double cacheTimeStart = System.currentTimeMillis();

        numTriples = queryTriplesWithID.size();

        for (Triple triple : queryTriplesWithID) {
            worqTable = false;
            String propNameWithID = handler.parse(triple.getPredicate().toString());
            String propName = handler.parse(triple.getPredicate().toString()).split("\\_\\d+")[0];
            String tableName = metadata.getTableName(propName);
            Dataset<Row> table;

            if (!tableName.equals(propName)) {
                propTableNames.put(propName, tableName);
                worqTable = true;
            }
            if (worqTable) {
                table = loadWORQTable(tableName, propName, propTableNames, queryJoins, tablesStats);
            } else {
                Tuple3<Boolean, Dataset<Row>, Long> res = loadTripleTable(tableName, propName, propNameWithID, queryJoins, tablesStats);
                isWarm = res._1();
                table = res._2();
                reductionSizes += res._3();
                tableSizes += Long.parseLong(metadata.getVpTableInfo(tableName).get("numTuples").toString());
            }
            dataFrames.put(propNameWithID, table);
        }

        //Handle Unbound triples
        if (unboundTriples.size() > 0) {
            numTriples += unboundTriples.size();
            opRoot = processUnboundProperties(unboundTriples, queryJoins, queryTriplesWithID, dataFrames, lastID, tablesStats);
        }

        cacheTime = System.currentTimeMillis() - cacheTimeStart;

        Dataset<Row> generatedPlan;
        int uT = unboundTriples.size();
        int qT = queryTriplesWithID.size();
        int total = uT + qT;

        if (total > 1) {
            planner.setTablesStatistics(tablesStats);
            opRoot = planner.bgpOptimizeStats(opRoot);
        }

        visitor = new BGPVisitorByID(metadata.getBenchmarkName());
        OpWalker.walk(opRoot, visitor);
        generatedPlan = planner.evaluate(visitor.getTriplesWithID(), dataFrames);
        numResults = generatedPlan.count();

        double time = System.currentTimeMillis() - start;

//        for (Map.Entry<String, Dataset<Row>> df : dataFrames.entrySet()) {
//            df.getValue().unpersist();
//        }
//        dataFrames.clear();

        return new WORQResult(numResults, time, tableSizes, reductionSizes, maxJoins, isWarm, numTriples);
    }

    private Dataset<Row> loadWORQTable(String tableName, String propName, HashMap<String, String> propTableNames, HashMap<String, ArrayList<String>> queryJoins, List<Map> tablesStats) {

        SemanticFilter filter = new SemanticFilter(this.metadata);
        String semfilter;
        semfilter = filter.getSemanticFilter(propName, propTableNames, queryJoins);
        Dataset<Row> table;
        if (semfilter != null) {
            table = spark.read().parquet(metadata.getHdfsDbPath() + tableName)
                    .select(col("sub"), col("obj"), col(semfilter)).filter(col(semfilter).isNotNull()).select(col("sub"), col("obj"));
            LOG.debug("\t\tUSING SEMANTIC FILTER : " + semfilter);
            int numTup = metadata.getSemanticColumnCount(propTableNames.get(propName), semfilter);
            HashMap<String, String> entry = new HashMap<>();
            entry.put("tableName", propName);
            entry.put("numTuples", String.valueOf(numTup));
            entry.put("numTriples", String.valueOf(metadata.DBInfo().get("numTriples")));
            entry.put("ratio", String.valueOf(0));
            tablesStats.add(entry);
            LOG.debug("\t\tSTAT COUNT: " + numTup);
        } else {
            table = spark.read().parquet(metadata.getHdfsDbPath() + tableName)
                    .select(col("sub"), col("obj"));
            HashMap<String, String> entry = new HashMap<>();
            entry.put("tableName", propName);
            entry.put("numTuples", String.valueOf(metadata.getWORQTableInfo(tableName).get("numTuples")));
            entry.put("numTriples", String.valueOf(metadata.DBInfo().get("numTriples")));
            entry.put("ratio", String.valueOf(metadata.getWORQTableInfo(tableName).get("ratio")));
            tablesStats.add(entry);
            LOG.debug("\t\tSTAT COUNT: " + String.valueOf(metadata.getWORQTableInfo(tableName).get("numTuples")));
        }
        return table;
    }

    public Tuple3<Boolean, Dataset<Row>, Long> loadTripleTable(String tableName, String propName, String propNameWithID, HashMap<String, ArrayList<String>> queryJoins, List<Map> tablesStats) {

        SemanticFilter filter = new SemanticFilter(this.metadata);
        HashMap<String, String> queryFilters = filter.identifySemanticFilters(propNameWithID, queryJoins);

        Dataset<Row> table;
        Dataset<Row> subEntries = null;
        Dataset<Row> objEntries = null;
        boolean isWarm = false;
        long reductionSize = 0;

        if (queryFilters.size() > 0) { //Compute or load reduction

            String subjectEntry = queryFilters.get("sub");
            String objectEntry = queryFilters.get("obj");

            if (subjectEntry != null) {
                LOG.debug("Unbound subject");
                Tuple3<Boolean, Dataset<Row>, Long> res = getEntries("sub", subjectEntry, tableName, propNameWithID);
                isWarm = res._1();
                subEntries = res._2();
                reductionSize = res._3();
            }

            if (objectEntry != null && subjectEntry == null) {
                LOG.debug("Unbound object (No subject)");
                Tuple3<Boolean, Dataset<Row>, Long> res = getEntries("obj", objectEntry, tableName, propNameWithID);
                isWarm = res._1();
                objEntries = res._2();
                reductionSize = res._3();
            }

//            if (origPartition != null) {
//                origPartition.unpersist();
//            }

            //Add the data to cache
            if (subjectEntry != null && objectEntry != null) { //Has a subject and object filter , then computer thier intersection
                LOG.debug("Unbound subject + object");
//                Dataset<Row> subs = subEntries.alias("subs");
//                Dataset<Row> objs = objEntries.alias("objs");
//                Column condition = col("subs.sub").equalTo(col("objs.sub")).and(col("subs.obj").equalTo(col("objs.obj")));
//                Dataset<Row> intersection = subs.join(objs, condition, "inner").select(col("subs.sub").as("sub"), col("subs.obj").as("obj"));
//                long countSub = subEntries.count(); //Allows us to select which table as a representative for the relation
//                long countObj = objEntries.count();
//                if (countSub < countObj)
                table = subEntries.repartition(col("sub"));
//                else
//                  table = objEntries.repartition(col("sub"));
            } else if (subjectEntry != null) {
                table = subEntries.repartition(col("sub"));
            } else {
                table = objEntries.repartition(col("obj"));
            }
        } else { //Load original partition
            LOG.debug(">> Using VP for " + propName);
            if (!cachedTables.containsKey(tableName)) {
                LOG.debug("Loading table");
                table = spark.read().parquet(metadata.getHdfsDbPath() + tableName);
                cachedTables.put(tableName, table);
            } else {
                LOG.debug("Using existing cached table");
                table = cachedTables.get(tableName);
            }
            reductionSize = Long.parseLong(metadata.getVpTableInfo(tableName).get("numTuples").toString());
        }
        table.cache(); //Caching step
        //cachedTables.add(propNameWithID);
        //reductionsSizes += reduction;
        HashMap<String, String> entry = new HashMap<>();
        entry.put("tableName", propNameWithID);
        entry.put("numTuples", String.valueOf(reductionSize));
        entry.put("numTriples", String.valueOf(metadata.DBInfo().get("numTriples")));
        entry.put("ratio", String.valueOf(0));
        tablesStats.add(entry);

        return new Tuple3(isWarm, table, reductionSize);
    }

    public Tuple3<Boolean, Dataset<Row>, Long> getEntries(String col, String triplePart, String tableName, String propNameWithID) {

        Dataset<Row> tripleEntries = null;
        Dataset<Row> origPartition = null;
        long origNumEntries = Long.valueOf(metadata.getVpTableInfo(tableName).get("numTuples").toString());

        boolean isWarm = false;
        long numEntries = 0;
        SemanticFilter filter = new SemanticFilter(metadata);

        if (semanticFilters.containsKey(triplePart)) {
            LOG.debug("Reading existing " + triplePart);
            isWarm = true;
            tripleEntries = semanticFilters.get(triplePart);
            numEntries = statistics.get(triplePart);
        } else {
//            if (!cachedTables.containsKey(tableName)) {
                origPartition = spark.read().parquet(metadata.getHdfsDbPath() + tableName);
//                origPartition.cache();
                LOG.debug("[" + tableName + "] Before Reduction: " + origNumEntries);
//                cachedTables.put(tableName, origPartition);
//            } else {
//                LOG.debug("Reading Original Cached Table for " + triplePart);
//                origPartition = cachedTables.get(tableName);
//            }
            LOG.debug("Computing Reduction for " + propNameWithID);
            tripleEntries = filter.computeDataset(propNameWithID, col, triplePart, origPartition);
            //tripleEntries.cache();
            numEntries = tripleEntries.count();
            LOG.debug("[" + tableName + "] After Reduction: " + numEntries);
            statistics.put(triplePart, numEntries);

//            if (numEntries == origNumEntries) {
//                LOG.debug("Same Reduction Size as Original, Using Original Partition instead");
//                semanticFilters.put(triplePart, origPartition);
//                return new Tuple3(isWarm, origPartition, origNumEntries);
//            } else {
//                LOG.debug("Saving Semantic Filter " + triplePart);
                semanticFilters.put(triplePart, tripleEntries);
//            }
        }
        return new Tuple3(isWarm, tripleEntries, numEntries);
    }

    public Op processUnboundProperties(ArrayList<Triple> unboundTriples, HashMap<String, ArrayList<String>> queryJoins, ArrayList<Triple> queryTriplesWithID, HashMap<String, Dataset<Row>> dataFrames, int lastID, List<Map> tablesStats) {

        BasicPattern res = new BasicPattern();
        for (Triple triple : unboundTriples) {

            HashSet<String> candidateProperties;

            if (triple.getSubject().isVariable() && triple.getObject().isVariable() && queryJoins.size() > 0) {
                candidateProperties = identifyUnboundPropertiesByJoin(triple, queryJoins, dataFrames);
            } else {
                candidateProperties = identifyUnboundPropertiesByFilters(triple);
            }

            //////// Construct new BGP ////////////////
            for (Triple tid : queryTriplesWithID) {
                res.add(tid);
            }

            Triple tr = verifyAndLoadUnboundTriple(triple, candidateProperties, dataFrames, tablesStats, lastID);
            res.add(tr);
            //////////////////////////////////////////
        }
        return new OpBGP(res);
    }

    private HashSet<String> identifyUnboundPropertiesByJoin(Triple triple, HashMap<String, ArrayList<String>> queryJoins, HashMap<String, Dataset<Row>> dataFrames) {

        HashSet<String> candidateProperties = new HashSet<>();
        Set<String> allProperties = new HashSet<>();

        if (triple.getSubject().isVariable() && queryJoins.get(triple.getSubject().getName()) != null) {
            ArrayList<String> propList = queryJoins.get(triple.getSubject().getName());
            allProperties.addAll(propList);
        }

        if (triple.getObject().isVariable() && queryJoins.get(triple.getObject().getName()) != null) {
            ArrayList<String> propList = queryJoins.get(triple.getObject().getName());
            allProperties.addAll(propList);
        }

        for (String candidateProp : allProperties) {
            String[] parts = candidateProp.split("_TRP");
            String propWithID = parts[0];
            String column = parts[1];
            String columnName;

            if (column.equals("S"))
                columnName = "sub";
            else
                columnName = "obj";

            String prop = propWithID.split("\\_\\d+")[0];
            Dataset<Row> propEntries = dataFrames.get(propWithID);
            JavaRDD<String> results = propEntries.toJavaRDD().flatMap(new FlatMapFunction<Row, String>() {
                @Override
                public Iterator<String> call(Row row) throws Exception {
                    Set<String> bloomFilterNames = metadata.getBroadcastFilters().getValue().keySet();
                    ArrayList pNames = new ArrayList<>();
                    for (String p : bloomFilterNames) {
                        if (!p.equals(prop)) {
                            boolean subExists = false;
                            boolean objExists = false;
                            if (columnName.equals("sub")) {
                                BloomFilter f = metadata.getBroadcastFilters().getValue().get(p).get("sub");
                                subExists = f.contains((String) row.getAs("sub"));
                            }
                            if (columnName.equals("obj")) {
                                BloomFilter f = metadata.getBroadcastFilters().getValue().get(p).get("obj");
                                objExists = f.contains((String) row.getAs("obj"));
                            }
                            if (subExists || objExists)
                                pNames.add(p);
                        }
                    }
                    return pNames.iterator();
                }
            });
            if (candidateProperties.size() == 0)
                candidateProperties.addAll(results.distinct().collect());
            else
                candidateProperties.retainAll(results.distinct().collect());
        }
        return candidateProperties;
    }

    private Triple verifyAndLoadUnboundTriple(Triple triple, HashSet<String> identifiedProperties, HashMap<String, Dataset<Row>> dataFrames, List<Map> tablesStats, int lastID) {
        ArrayList<String> verifiedList = new ArrayList<>();
        NameSpaceHandler handler = new NameSpaceHandler(metadata.getBenchmarkName());
        String subject = handler.parse(triple.getSubject().toString());
        String object = handler.parse(triple.getObject().toString());
        Triple tr;
        //Loading & Verification step
        for (String pr : identifiedProperties) {
            //String expandedPropNameWithID = handler.expandPrefix(pr) + "_" + lastID;
            String propNameWithID = pr + "_" + lastID;
            String tableName = metadata.getTableName(pr);
            //Triple tr = new Triple(triple.getSubject(), NodeFactory.createURI(expandedPropNameWithID), triple.getObject());
            Dataset<Row> table;

            if (!cachedTables.containsKey(tableName)) {
                table = spark.read().parquet(metadata.getHdfsDbPath() + tableName);
                table.persist(StorageLevel.MEMORY_ONLY());
                cachedTables.put(tableName, table);
            } else {
                table = cachedTables.get(tableName);
            }

            if (!triple.getSubject().isVariable() && !triple.getObject().isVariable()) {
                table = table.filter(col("sub").equalTo(subject).and(col("obj").equalTo(object)));
            } else if (!triple.getSubject().isVariable() && triple.getObject().isVariable()) {
                table = table.filter(col("sub").equalTo(subject));
            } else {
                table = table.filter(col("obj").equalTo(object));
            }
            table.persist(StorageLevel.MEMORY_ONLY());
            long size = table.count();
            if (size > 0) {
                verifiedList.add(propNameWithID);
                dataFrames.put(propNameWithID, table);
                //res.add(tr);
                lastID += 1;
            } else {
                table.unpersist();
            }
        }

        if (verifiedList.size() > 1) { //Union the verified properties into one table
            Dataset<Row> candidate = dataFrames.remove(verifiedList.get(0));
            lastID -= 1;
            for (int i = 1; i < verifiedList.size(); i++) {
                Dataset<Row> r = dataFrames.remove(verifiedList.get(i));
                candidate = candidate.union(r);
                r.unpersist();
                lastID -= 1;
            }
            candidate.persist(StorageLevel.MEMORY_ONLY());
            String unionPropID = "union" + "_" + lastID;
            dataFrames.put(unionPropID, candidate);
            tr = new Triple(triple.getSubject(), NodeFactory.createURI(unionPropID), triple.getObject());
            HashMap<String, String> entry = new HashMap<>();
            entry.put("tableName", verifiedList.get(0));
            entry.put("numTuples", String.valueOf(candidate.count()));
            entry.put("numTriples", String.valueOf(metadata.DBInfo().get("numTriples")));
            entry.put("ratio", String.valueOf(0));
            tablesStats.add(entry);
        } else { //Create a triple for this one property only
            String expandedPropNameWithID = handler.expandPrefix(verifiedList.get(0));
            tr = new Triple(triple.getSubject(), NodeFactory.createURI(expandedPropNameWithID), triple.getObject());
            HashMap<String, String> entry = new HashMap<>();
            entry.put("tableName", verifiedList.get(0));
            entry.put("numTuples", String.valueOf(dataFrames.get(verifiedList.get(0)).count()));
            entry.put("numTriples", String.valueOf(metadata.DBInfo().get("numTriples")));
            entry.put("ratio", String.valueOf(0));
            tablesStats.add(entry);
        }
        return tr;
    }

    public HashSet<String> identifyUnboundPropertiesByFilters(Triple triple) {

        HashSet<String> subIdentifiedProperties = new HashSet<>();
        HashSet<String> objIdentifiedProperties = new HashSet<>();

        NameSpaceHandler handler = new NameSpaceHandler(metadata.getBenchmarkName());
        String subject = handler.parse(triple.getSubject().toString());
        String object = handler.parse(triple.getObject().toString());

        Set<String> bloomFilterNames = metadata.getBloomFilters().keySet();
        for (String p : bloomFilterNames) {
            if (!triple.getSubject().isVariable()) {
                BloomFilter f = metadata.getBloomFilters().get(p).get("sub");
                boolean subExists = f.contains(subject);
                if (subExists) {
                    subIdentifiedProperties.add(p);
                }
            }
            if (!triple.getObject().isVariable()) {
                BloomFilter f = metadata.getBloomFilters().get(p).get("obj");
                boolean objExists = f.contains(object);
                if (objExists) {
                    objIdentifiedProperties.add(p);
                }
            }
        }
        if (subIdentifiedProperties.size() > 0 && objIdentifiedProperties.size() > 0) {
            subIdentifiedProperties.retainAll(objIdentifiedProperties);
            return subIdentifiedProperties;
        } else if (subIdentifiedProperties.size() > 0) {
            return subIdentifiedProperties;
        } else {
            return objIdentifiedProperties;
        }
    }

    public void close() {
    }
}
