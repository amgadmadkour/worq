package edu.purdue.worq.queryprocessing.execution;


import org.apache.jena.sparql.algebra.Op;

/**
 * @author Amgad Madkour
 */
public interface WORQExecutionEngine {
	WORQResult execute(Op sparqlOp);

    void close();
}
