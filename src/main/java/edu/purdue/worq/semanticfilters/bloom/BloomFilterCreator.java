package edu.purdue.worq.semanticfilters.bloom;

import edu.purdue.worq.metadata.WORQMetadata;
import orestes.bloomfilter.BloomFilter;
import orestes.bloomfilter.FilterBuilder;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Row;

import java.io.*;
import java.util.Iterator;

/**
 * @author Amgad Madkour
 */
public class BloomFilterCreator {

	private WORQMetadata metadata;
	private long size;
	private static final Logger LOG = Logger.getLogger(BloomFilterCreator.class.getName());

	public BloomFilterCreator(WORQMetadata metadata, long size) {
		Logger.getLogger(LOG.getName()).setLevel(Level.DEBUG);
		this.metadata = metadata;
		this.size = size;
	}

    public void createRedisBloomFilter(String bloomFilterName, Iterator<Row> elems) {
        BloomFilter bf = new FilterBuilder((int) this.size, this.metadata.getFalsePositiveProbability())
                //.hashFunction(this.metadata.getHashMethod().getHashFunction())
                .buildBloomFilter();
		//DynamicBloomFilter subDBF = new DynamicBloomFilter();
        while (elems.hasNext()) {
            bf.add(elems.next().getString(0));
            //subDBF.add(new Key(sub.getString(0).getBytes()));
		}
		saveBloomFilter(bf, bloomFilterName);
	}

	public void saveBloomFilter(Object bf, String bloomFilterName) {
		FileOutputStream fout = null;
		ObjectOutputStream oos = null;
		try {

			File directory = new File(this.metadata.getLocalDbPath() + "filters/");

			if (!directory.exists())
				directory.mkdir();

			fout = new FileOutputStream(this.metadata.getLocalDbPath() + "filters/" + bloomFilterName);
			oos = new ObjectOutputStream(fout);
			//JsonElement subJson = BloomFilterConverter.toJson(subBF);
			oos.writeObject(bf);
			//oos.writeObject(subJson.toString());
			oos.close();
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public void createSparkBloomFilter(Object bf, String filterName) {
		saveBloomFilter(bf, filterName);
	}

	public long getSize() {
		return size;
	}

	public void setSize(long size) {
		this.size = size;
	}
}
