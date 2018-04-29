package edu.purdue.worq.storage;

import edu.purdue.worq.rdf.RDFDataset;

/**
 * @author Amgad Madkour
 */
public interface WORQStoreCreator {

	boolean loadFromHDFS(String datasetPath,
	                     String separator,
	                     String localdbPath,
	                     String hdfsdbPath,
	                     RDFDataset RDFDatasetName);
}
