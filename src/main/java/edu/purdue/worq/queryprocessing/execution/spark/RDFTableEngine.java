package edu.purdue.worq.queryprocessing.execution.spark;

import edu.purdue.worq.metadata.WORQMetadata;
import edu.purdue.worq.rdf.BGPVisitor;
import edu.purdue.worq.queryprocessing.execution.WORQExecutionEngine;
import edu.purdue.worq.queryprocessing.execution.WORQResult;
import edu.purdue.worq.rdf.NameSpaceHandler;
import org.apache.jena.graph.Triple;
import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.algebra.OpWalker;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.io.Serializable;
import java.util.ArrayList;

import static org.apache.spark.sql.functions.col;

/**
 * @author Amgad Madkour
 */
public class RDFTableEngine implements Serializable, WORQExecutionEngine {

    private static Logger LOG = Logger.getLogger(RDFTableEngine.class.getName());
    private static SparkSession spark;
    private WORQMetadata metadata;

    public RDFTableEngine(WORQMetadata metadata) {
        Logger.getLogger(LOG.getName()).setLevel(Level.DEBUG);
        this.metadata = metadata;
        SparkConf conf = new SparkConf()
                .setAppName("RDFTableEngine")
                .set("spark.executor.memory", "20g")
                .set("spark.sql.inMemoryColumnarStorage.batchSize", "20000")
                .set("spark.sql.shuffle.partitions", String.valueOf(metadata.getNumberOfCores()));
        spark = SparkSession
                .builder().config(conf).getOrCreate();
        this.metadata.setSparkSession(spark);
    }

    public WORQResult execute(Op opRoot) {

        double start = System.currentTimeMillis();
        //Load the triple dataset
        String tableName = "base.parquet";
        Dataset<Row> table = spark.read().parquet(metadata.getHdfsDbPath() + tableName);
        NameSpaceHandler handler = new NameSpaceHandler(metadata.getBenchmarkName());

        BGPVisitor visitor = new BGPVisitor(metadata.getBenchmarkName());
        OpWalker.walk(opRoot, visitor);
        ArrayList<Triple> triples = visitor.getTriples();

        Triple t = triples.get(0); //For demonstration purposes, we just have one triple for unbound predicate query

        String subject = handler.parse(t.getSubject().toString());
        String object = handler.parse(t.getObject().toString());

        if (t.getSubject().isVariable() && t.getPredicate().isVariable() && !t.getObject().isVariable()) {
            table = table.where(col("obj").equalTo(object));
        }
        if (!t.getSubject().isVariable() && t.getPredicate().isVariable() && t.getObject().isVariable()) {
            table = table.where(col("sub").equalTo(subject));
        }
        if (!t.getSubject().isVariable() && t.getPredicate().isVariable() && !t.getObject().isVariable()) {
            table = table.where(col("sub").equalTo(subject)
                    .and(col("obj").equalTo(object)));
        }


        long numResults = table.count();
        double time = System.currentTimeMillis() - start;

        return new WORQResult(numResults, time, 0, 0, 0, false, 0);
    }

    @Override
    public void close() {

    }

}
