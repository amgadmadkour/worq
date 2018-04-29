package edu.purdue.worq.rdf;

import org.apache.jena.graph.Node;
import org.apache.jena.graph.NodeFactory;
import org.apache.jena.graph.Triple;
import org.apache.jena.sparql.algebra.OpVisitorBase;
import org.apache.jena.sparql.algebra.op.OpBGP;
import org.apache.jena.sparql.algebra.op.OpProject;
import org.apache.jena.sparql.algebra.op.OpTriple;
import org.apache.jena.sparql.core.BasicPattern;
import org.apache.jena.sparql.core.Var;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;


import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

/**
 * @author Amgad Madkour
 */
public class BGPVisitorByID extends OpVisitorBase {

	private ArrayList<String> properties;
	private ArrayList<String> queryVars;
	private HashMap<String, ArrayList<String>> joinVariableEntries;
	private ArrayList<Triple> triples;
    private HashMap<String, String> bgpReplacements;
    private ArrayList<Triple> triplesWithID;
    private static Logger LOG = Logger.getLogger(BGPVisitorByID.class.getName());
	private NameSpaceHandler nsHandler;
    private int ID;
    private ArrayList<Triple> unboundTriples;

	public BGPVisitorByID(RDFDataset ns) {
		Logger.getLogger(LOG.getName()).setLevel(Level.DEBUG);
		this.properties = new ArrayList<>();
		this.joinVariableEntries = new HashMap<>();
		this.bgpReplacements = new HashMap<>();
		this.ID = 0;
		this.nsHandler = new NameSpaceHandler(ns);
		this.triples = new ArrayList<>();
        this.queryVars = new ArrayList<>();
        this.triplesWithID = new ArrayList<>();
        this.unboundTriples = new ArrayList<>();
    }

	@Override
	public void visit(OpProject opProject) {
		List<Var> vars = opProject.getVars();

		for (Var v : vars) {
			this.queryVars.add(v.getVarName());
		}
	}

	@Override
	public void visit(OpBGP opBGP) {
		BasicPattern p = opBGP.getPattern();
		for (Triple t : p) {
            if (t.getPredicate().isURI()) {
                this.ID += 1;
                String origSub = t.getSubject().toString();
                String sub = nsHandler.parse(t.getSubject().toString());
                String pred = nsHandler.parse(t.getPredicate().toString());
                String origPred = t.getPredicate().toString();
                String predID = pred + "_" + ID;
                String origObj = t.getObject().toString();
                String obj = nsHandler.parse(t.getObject().toString());
                Triple t1 = new Triple(t.getSubject(), NodeFactory.createURI(predID), t.getObject());

                if (origSub.contains("http")) {
                    origSub = "<" + origSub + ">";
                }
                if (origPred.contains("http")) {
                    origPred = "<" + origPred + ">";
                }
                if (origObj.contains("http")) {
                    origObj = "<" + origObj + ">";
                }

                if (sub.contains("http")) {
                    sub = "<" + sub + ">";
                }

                if (obj.contains("http")) {
                    obj = "<" + obj + ">";
                }

                String original = origSub.replace("__", ":") + " " + origPred.toString().replace("__", ":") + " " + origObj.toString().replace("__", ":");
                String replacement = sub.replace("__", ":") + " " + predID.replace("__", ":") + " " + obj.replace("__", ":");
                this.triplesWithID.add(new Triple(t.getSubject(), NodeFactory.createURI(t.getPredicate().toString()), t.getObject()));
                bgpReplacements.put(original, replacement);

                Node prop = t1.getPredicate();
                this.properties.add(pred);

                Node subject = t1.getSubject();
                if (subject.isVariable()) {
                    if (prop.isURI()) {
                        if (this.joinVariableEntries.containsKey(subject.getName())) {
                            this.joinVariableEntries.get(subject.getName()).add(pred + "_TRPS");
                        } else {
                            ArrayList<String> newList = new ArrayList<>();
                            newList.add(pred + "_TRPS");
                            this.joinVariableEntries.put(subject.getName(), newList);
                        }
                    }
                }

                Node object = t1.getObject();
                if (object.isVariable()) {
                    if (prop.isURI()) {
                        if (this.joinVariableEntries.containsKey(object.getName())) {
                            this.joinVariableEntries.get(object.getName()).add(pred + "_TRPO");
                        } else {
                            ArrayList<String> newList = new ArrayList<>();
                            newList.add(pred + "_TRPO");
                            this.joinVariableEntries.put(object.getName(), newList);
                        }
                    }
                }
            } else {
                unboundTriples.add(t);
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

	public HashMap<String, String> getBgpReplacements() {
		return this.bgpReplacements;
	}

	public ArrayList<Triple> getTriples() {
        return this.triples;
    }

    public ArrayList<Triple> getTriplesWithID() {
        return triplesWithID;
    }

    public ArrayList<Triple> getUnboundTriples() {
        return this.unboundTriples;
    }

    public int getLastID() {
        return this.ID;
    }

}