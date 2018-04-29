package edu.purdue.worq.rdf;


import org.apache.jena.shared.PrefixMapping;
import org.apache.jena.shared.impl.PrefixMappingImpl;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import java.io.*;
import java.util.HashMap;
import java.util.Map;


/**
 * @author Amgad Madkour
 */
public class NameSpaceHandler implements Serializable {

    private static Logger LOG = Logger.getLogger(NameSpaceHandler.class.getName());
    private HashMap<String, String> prefixList;

	public NameSpaceHandler(RDFDataset ns) {
        Logger.getLogger(LOG.getName()).setLevel(Level.DEBUG);
        prefixList = new HashMap<>();
		loadNameSpace(ns);
	}

	private void loadNameSpace(RDFDataset ns) {
		try {
			//ClassLoader classLoader = getClass().getClassLoader();
			//File file = new File(classLoader.getResource("prefixes/dbpedia.txt").getFile());
			//InputStream jarStream = Thread.currentThread().getContextClassLoader().getResourceAsStream("prefixes/dbpedia.txt");
			InputStream jarStream = null;
			if (ns == RDFDataset.DBPEDIA) {
				jarStream = Thread.currentThread().getContextClassLoader().getResourceAsStream("prefixes/dbpedia.txt");
			} else if (ns == RDFDataset.WATDIV) {
				jarStream = Thread.currentThread().getContextClassLoader().getResourceAsStream("prefixes/watdiv.txt");
			} else if (ns == RDFDataset.YAGO) {
				jarStream = Thread.currentThread().getContextClassLoader().getResourceAsStream("prefixes/yago.txt");
			} else if (ns == RDFDataset.LUBM) {
				jarStream = Thread.currentThread().getContextClassLoader().getResourceAsStream("prefixes/lubm.txt");
			}
			if (ns == RDFDataset.GENERIC) {
				return;
			}
			BufferedReader rdr = new BufferedReader(new InputStreamReader(jarStream));
			String temp;
			while ((temp = rdr.readLine()) != null) {
				String[] parts = temp.split("\t");
				prefixList.put(parts[1], parts[0]);
			}
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public String expandPrefix(String shortForm) {
		String[] parts = shortForm.split("__");
		for (Map.Entry entry : prefixList.entrySet()) {
			if (entry.getValue().equals(parts[0])) {
				return entry.getKey() + parts[1];
			}
		}
		return null;
	}

	private String identifyPrefix(String uri) {
		for (String key : prefixList.keySet()) {
			if (uri.contains(key)) {
				return uri.replace(key, prefixList.get(key) + "__");
			}
		}
		return uri;
	}

	public String parse(String uri) {
		if (uri.startsWith("<") && uri.endsWith(">")) {
			uri = uri.substring(1, uri.length() - 1);
		} else if (uri.startsWith("\"") && uri.endsWith("\"")) {
			return uri;
		}
		return identifyPrefix(uri);
	}

	public PrefixMapping getPrefixMapping() {

		PrefixMapping mapping = new PrefixMappingImpl();

		for (Map.Entry<String, String> entry : prefixList.entrySet()) {
			mapping.setNsPrefix(entry.getValue(), entry.getKey());
		}
		return mapping;
	}
}
