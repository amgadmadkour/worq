package edu.purdue.worq.rdf;


import org.apache.jena.graph.Node;
import org.apache.jena.graph.NodeFactory;
import org.apache.jena.graph.Triple;
import org.apache.jena.sparql.algebra.OpVisitorBase;
import org.apache.jena.sparql.algebra.op.OpBGP;
import org.apache.jena.sparql.algebra.op.OpTriple;
import org.apache.jena.sparql.core.BasicPattern;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;


import java.util.ArrayList;
import java.util.HashMap;

/**
 * @author Amgad Madkour
 */
public class BGPVisitor extends OpVisitorBase {

	private ArrayList<String> properties;
	private HashMap<String, ArrayList<String>> joinVariableEntries;
	private static Logger LOG = Logger.getLogger(BGPVisitorByID.class.getName());
	private NameSpaceHandler nsHandler;
	private HashMap<String, Triple> propertyTriples;
	private ArrayList<Triple> triples;
	int ID;

	public BGPVisitor(RDFDataset ns) {
		Logger.getLogger(LOG.getName()).setLevel(Level.DEBUG);
		this.properties = new ArrayList<>();
		this.joinVariableEntries = new HashMap<>();
		this.nsHandler = new NameSpaceHandler(ns);
		this.triples = new ArrayList<>();
		this.propertyTriples = new HashMap<>();
	}

	@Override
	public void visit(OpBGP opBGP) {
		BasicPattern p = opBGP.getPattern();
		for (Triple t : p) {
			this.triples.add(t);
			String pred = nsHandler.parse(t.getPredicate().toString());
			Triple t1 = new Triple(t.getSubject(), NodeFactory.createURI(pred), t.getObject());
			this.propertyTriples.put(pred, t1);

			Node subject = t1.getSubject();
			if (subject.isVariable()) {
				if (this.joinVariableEntries.containsKey(subject.getName())) {
					this.joinVariableEntries.get(subject.getName()).add(pred + "_TRPS");
				} else {
					ArrayList<String> newList = new ArrayList<>();
					newList.add(pred + "_TRPS");
					this.joinVariableEntries.put(subject.getName(), newList);
				}
			}

			Node prop = t1.getPredicate();
			if (prop.isURI()) {
				this.properties.add(pred);
			}

			Node object = t1.getObject();
			if (object.isVariable()) {
				if (this.joinVariableEntries.containsKey(object.getName())) {
					this.joinVariableEntries.get(object.getName()).add(pred + "_TRPO");
				} else {
					ArrayList<String> newList = new ArrayList<>();
					newList.add(pred + "_TRPO");
					this.joinVariableEntries.put(object.getName(), newList);
				}
			}
		}
	}

	@Override
	public void visit(OpTriple opTriple) {
	}

	public HashMap<String, ArrayList<String>> getJoinVariableEntries() {
		return joinVariableEntries;
	}

	public ArrayList<String> getProperties() {
		return this.properties;
	}

	public ArrayList<Triple> getTriples() {
		return triples;
	}

	public HashMap<String, Triple> getPropertyTriples() {
		return this.propertyTriples;
	}

}