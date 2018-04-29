package edu.purdue.worq.metadata;

import edu.purdue.worq.rdf.RDFDataset;
import edu.purdue.worq.queryprocessing.execution.ExecutionEngine;
import edu.purdue.worq.storage.ComputationType;
import edu.purdue.worq.utils.Separator;
import orestes.bloomfilter.HashProvider.HashMethod;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.SparkSession;
import org.yaml.snakeyaml.DumperOptions;
import org.yaml.snakeyaml.Yaml;

import java.io.*;
import java.util.*;

//import org.apache.spark.util.sketch.BloomFilter;
import orestes.bloomfilter.BloomFilter;

/**
 * @author Amgad Madkour
 */
public class WORQMetadata implements Serializable {

	//LOGGER
	private static Logger LOG = Logger.getLogger(WORQMetadata.class.getName());

	private String datasetPath;
	private String datasetSeparator;
	private ComputationType store;
	private RDFDataset benchmarkName;
	private String queryFilePath;
	private Map dbInfo;
	private List<Map> vpTablesList;
	private List<Map> worqTablesList;
	private HashMap<String, HashMap<String, BloomFilter>> bloomFilters;
	private long numQueries;
	private String inputDirPath;
	private ExecutionEngine engine;
	private HashMethod hashMethod;
	private int numberOfCores;
	private String currentQueryName;
	private SparkSession spark;
	private Broadcast<HashMap<String, HashMap<String, BloomFilter>>> broadcastFilters;
    private boolean isQueryBatch;

	public WORQMetadata() {
		this.dbInfo = new HashMap<>();
		this.vpTablesList = new ArrayList<>();
		this.worqTablesList = new ArrayList<>();
		this.bloomFilters = new HashMap<>();
		this.spark = null;
		Logger.getLogger(LOG.getName()).setLevel(Level.DEBUG);
	}

	public HashMap<String, HashMap<String, BloomFilter>> getBloomFilters() {
		return this.bloomFilters;
	}

	public String getHdfsDbPath() {
		return this.dbInfo.get("hdfsDbPath").toString();
	}

	public void setHdfsDbPath(String hdfsDbPath) {
		this.dbInfo.put("hdfsDbPath", hdfsDbPath);
	}

	public String getLocalDbPath() {
		return this.dbInfo.get("localDbPath").toString();
	}

	public void setLocalDbPath(String localDbPath) {
		this.dbInfo.put("localDbPath", localDbPath);
	}

	public void loadMetaData() {
		Yaml databaseInfo = new Yaml();
		Yaml vpTablesInfo = new Yaml();
		Yaml worqTablesInfo = new Yaml();

		try {
			FileReader dbReader = new FileReader(getLocalDbPath() + "/dbinfo.yaml");
			this.dbInfo = (Map) databaseInfo.load(dbReader);
			LOG.debug(this.dbInfo);
			FileReader vpTablesRdr = new FileReader(getLocalDbPath() + "/vptables.yaml");
			Iterator<Object> docs1 = vpTablesInfo.loadAll(vpTablesRdr).iterator();
			while (docs1.hasNext()) {
				this.vpTablesList.add((Map) docs1.next());
			}
			FileReader worqTablesRdr = new FileReader(getLocalDbPath() + "/worqtables.yaml");
			Iterator<Object> docs2 = worqTablesInfo.loadAll(worqTablesRdr).iterator();
			while (docs2.hasNext()) {
				this.worqTablesList.add((Map) docs2.next());
			}
		} catch (IOException exp) {
			exp.printStackTrace();
		}
	}

	public void broadcastFilters() {
		loadFilters();
		LOG.debug("Broadcasting Bloom Filters");
		JavaSparkContext jc = new JavaSparkContext(spark.sparkContext());
		this.broadcastFilters = jc.broadcast(bloomFilters);
	}

	public void loadFilters() {
		File folder = new File(getLocalDbPath() + "filters/");
		File[] listOfFiles = folder.listFiles();

		LOG.debug("Loading " + listOfFiles.length + " filters");

		for (int i = 0; i < listOfFiles.length; i++) {
			if (listOfFiles[i].isFile()) {
				String name = listOfFiles[i].getName();
				String propertyName;
				boolean sub;
				if (name.startsWith("sub_")) {
					String[] parts = name.split("sub\\_");
					propertyName = parts[1];
					sub = true;
				} else {
					String[] parts = name.split("obj\\_");
					propertyName = parts[1];
					sub = false;
				}
				try {
					FileInputStream fin = new FileInputStream(new File(getLocalDbPath() + "filters/" + name));
					ObjectInputStream oos = new ObjectInputStream(fin);
					BloomFilter filter = (BloomFilter) oos.readObject();

					if (!sub && this.bloomFilters.containsKey(propertyName)) {
						this.bloomFilters.get(propertyName).put("obj", filter);
					} else if (sub && this.bloomFilters.containsKey(propertyName)) {
						this.bloomFilters.get(propertyName).put("sub", filter);
					} else if (!sub && !this.bloomFilters.containsKey(propertyName)) {
						HashMap<String, BloomFilter> entry = new HashMap<>();
						entry.put("obj", filter);
						this.bloomFilters.put(propertyName, entry);
					} else if (sub && !this.bloomFilters.containsKey(propertyName)) {
						HashMap<String, BloomFilter> entry = new HashMap<>();
						entry.put("sub", filter);
						this.bloomFilters.put(propertyName, entry);
					}
				} catch (ClassNotFoundException e) {
					e.printStackTrace();
				} catch (FileNotFoundException e) {
					e.printStackTrace();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}
	}

	public String getDatasetPath() {
		return this.datasetPath;
	}

	public void setDatasetPath(String datasetPath) {
		this.datasetPath = datasetPath;
	}

	public String getQueryFilePath() {
		return queryFilePath;
	}

    public void setQueryFilePath(String queryFilePath, boolean isBatch) {
        this.isQueryBatch = isBatch;
        this.queryFilePath = queryFilePath;
	}

	public String getDatasetSeparator() {
		return this.datasetSeparator;
	}

	public void setDatasetSeparator(Separator datasetSeparator) {
		if (datasetSeparator == Separator.TAB)
			this.datasetSeparator = "\\t";
		else if (datasetSeparator == Separator.SPACE)
			this.datasetSeparator = " ";
	}

	public ComputationType getStoreType() {
		return store;
	}

	public void setStore(ComputationType store) {
		this.store = store;
	}

	public RDFDataset getBenchmarkName() {
		return this.benchmarkName;
	}

	public void setBenchmarkName(RDFDataset benchmarkName) {
		this.benchmarkName = benchmarkName;
	}

	public Map<String, Object> DBInfo() {
		return this.dbInfo;
	}

	public List<Map> getVpTablesList() {
		return this.vpTablesList;
	}

	public Map getVpTableInfo(String tableName) {
		for (Map map : this.vpTablesList) {
			if (map.get("tableName").equals(tableName))
				return map;
		}
		return null;
	}

	public List<Map> getWORQTablesList() {
		return this.worqTablesList;
	}

	public boolean hasWORQTable(String tableName) {
		for (Map entry : worqTablesList) {
			if (entry.get("tableName").equals(tableName))
				return true;
		}
		return false;
	}

	public String getTableName(String property) {
		List<Map> worqList = getWORQTablesList();
		for (Map entry : worqList) {
			String tableName = entry.get("tableName").toString().split("WORQ\\_")[1];
			if (tableName.equals(property)) {
				return entry.get("tableName").toString();
			}
		}
		return property;
	}

	public void syncMetadata() {
		try {
			FileWriter databaseInfoWriter = new FileWriter(getLocalDbPath() + "dbinfo.yaml");
			FileWriter vpTableInfoWriter = new FileWriter(getLocalDbPath() + "vptables.yaml");
			FileWriter worqTableInfoWriter = new FileWriter(getLocalDbPath() + "worqtables.yaml");

			DumperOptions options = new DumperOptions();
			options.setSplitLines(false);
			options.setPrettyFlow(true);

			//YAML Objects
			Yaml yamlDBInfo = new Yaml(options);
			Yaml yamlVPTables = new Yaml(options);
			Yaml yamlWORQTables = new Yaml(options);

			yamlDBInfo.dump(this.dbInfo, databaseInfoWriter);
			yamlVPTables.dumpAll(this.vpTablesList.iterator(), vpTableInfoWriter);
			yamlWORQTables.dumpAll(this.worqTablesList.iterator(), worqTableInfoWriter);

			databaseInfoWriter.close();
			vpTableInfoWriter.close();
			worqTableInfoWriter.close();

		} catch (IOException exp) {
			exp.printStackTrace();
		}
	}

	public int getSemanticColumnCount(String tableName, String cand) {
		for (Map entry : this.worqTablesList) {
			if (entry.get("tableName").equals(tableName)) {
				if (entry.containsKey(cand)) {
					return (int) entry.get(cand);
				} else {
					return -1;
				}
			}
		}
		return -1;
	}

	public void setNumQueries(long numQueries) {
		this.numQueries = numQueries;
	}

	public long getNumQueries() {
		return this.numQueries;
	}

	public boolean addWORQColumn(String tableName, String columnName) {
		for (Map entry : this.worqTablesList) {
			if (entry.get("tableName").equals(tableName)) {
				if (entry.containsKey("schema")) {
					String schema = (String) entry.get("schema");
					schema = schema + "," + columnName;
					entry.put("schema", schema);
					return true;
				}
			}
		}
		return false;
	}

	public void removeVP(String partition) {
		ArrayList<Map> remSet = new ArrayList<>();
		for (Map entry : this.vpTablesList) {
			if (entry.get("tableName").equals(partition)) {
				remSet.add(entry);
			}
		}
		for (Map entry : remSet) {
			this.vpTablesList.remove(entry);
		}
	}

	public BloomFilter getVPBloomFilter(String tableName, String columnName) {
		return this.bloomFilters.get(tableName).get(columnName);
	}

	public Map getWORQTableInfo(String tableName) {
		for (Map map : this.worqTablesList) {
			if (map.get("tableName").equals(tableName))
				return map;
		}
		return null;
	}

	public void setInputDirPath(String inputDirPath) {
		this.inputDirPath = inputDirPath;
	}

	public String getInputDirPath() {
		return this.inputDirPath;
	}

	public ExecutionEngine getEngine() {
		return engine;
	}

	public void setEngine(ExecutionEngine engine) {
		this.engine = engine;
	}

	public HashMethod getHashMethod() {
		return hashMethod;
	}

	public void setHashMethod(HashMethod hashMethod) {
		this.hashMethod = hashMethod;
	}

	public double getFalsePositiveProbability() {
		return Double.parseDouble(this.dbInfo.get("falsePositiveProbability").toString());
	}

	public void setFalsePositiveProbability(double falsePositiveProbability) {
		DBInfo().put("falsePositiveProbability", falsePositiveProbability);
	}

	public void setNumberOfCores(int numberOfCores) {
		this.numberOfCores = numberOfCores;
	}

	public int getNumberOfCores() {
		return this.numberOfCores;
	}

	public void setCurrentQueryName(String currentQueryName) {
		this.currentQueryName = currentQueryName;
	}

	public String getCurrentQueryName() {
		return this.currentQueryName;
	}

	public long getBloomFilterSize() {
		return Long.parseLong(this.dbInfo.get("bloomFilterSize").toString());
	}

	public void setBloomFilterSize(long bloomFilterSize) {
		DBInfo().put("bloomFilterSize", bloomFilterSize);
	}

	public void setSparkSession(SparkSession spark) {
		this.spark = spark;
	}

	public SparkSession getSparkSession() {
		return this.spark;
	}

	public Broadcast<HashMap<String, HashMap<String, BloomFilter>>> getBroadcastFilters() {
		return broadcastFilters;
	}

	public void setBroadcastFilters(Broadcast<HashMap<String, HashMap<String, BloomFilter>>> broadcastFilters) {
		this.broadcastFilters = broadcastFilters;
	}

    public boolean isQueryBatch() {
        return this.isQueryBatch;
    }
}
