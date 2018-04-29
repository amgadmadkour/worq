package edu.purdue.worq.queryprocessing.optimization;

import edu.purdue.worq.metadata.WORQMetadata;
import edu.purdue.worq.rdf.NameSpaceHandler;
import edu.purdue.worq.utils.MapUtils;
import org.apache.jena.graph.Triple;
import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.algebra.TransformCopy;
import org.apache.jena.sparql.algebra.Transformer;
import org.apache.jena.sparql.algebra.op.OpBGP;
import org.apache.jena.sparql.core.BasicPattern;

import java.util.*;

/**
 * @author Amgad Madkour
 */
public class BGPOptimizerStats extends TransformCopy {

    private List<Map> tablesStatistics;
    private WORQMetadata metadata;

    public BGPOptimizerStats(WORQMetadata metadata, List<Map> tablesStatistics) {
        this.metadata = metadata;
        this.tablesStatistics = tablesStatistics;
    }

    public Op optimize(Op op) {
        return Transformer.transform(this, op);
    }

    public Op transform(OpBGP opBGP) {
        // if there are no more than 2 Triples -> reordering is useless
        if (opBGP.getPattern().size() <= 2) {
            return opBGP;
        }

        BasicPattern p = opBGP.getPattern();
        ArrayList<String> properties = new ArrayList<>();
        HashMap<String, Triple> propertyTriples = new HashMap<>();
        NameSpaceHandler handler = new NameSpaceHandler(this.metadata.getBenchmarkName());

        for (Triple t : p) {
            String propName = handler.parse(t.getPredicate().toString());
            properties.add(propName);
            propertyTriples.put(propName, t);
        }

        ArrayList<String> sortedProperties = sortByPropertySelectivity(properties);

        ArrayList<Triple> reorderedTriples = new ArrayList<>();
        ArrayList<Triple> sortedTriples = new ArrayList<>();
        int numTriples = opBGP.getPattern().size();

        for (String prop : sortedProperties) {
            sortedTriples.add(propertyTriples.get(prop));
        }

        //Re-sort so that the first triple be one with a literal
        Triple tpl = null;
        for (Triple t : sortedTriples) {
            if (!t.getSubject().isVariable() || !t.getObject().isVariable()) {
                tpl = t;
                break;
            }
        }

        if (tpl != null) {
            sortedTriples.remove(tpl);
            sortedTriples.add(0, tpl);
        }

        Triple firstTriple = sortedTriples.get(0);
        HashSet<String> joinVars = new HashSet<>();
        ArrayList<Triple> literalTriples = new ArrayList<>();
        ArrayList<Triple> joinedTriples;

        if (firstTriple.getSubject().isVariable())
            joinVars.add(firstTriple.getSubject().toString());
        else
            literalTriples.add(firstTriple);

        if (firstTriple.getObject().isVariable())
            joinVars.add(firstTriple.getObject().toString());
        else
            literalTriples.add(firstTriple);

        reorderedTriples.add(sortedTriples.get(0));
        sortedTriples.remove(0);

        while (reorderedTriples.size() != numTriples) {

            joinedTriples = new ArrayList<>();
            literalTriples = new ArrayList<>();
            boolean joinFound = false;
            for (int i = 0; i < sortedTriples.size(); i++) {
                Triple t2 = sortedTriples.get(i);
                boolean subJoins = false;
                boolean objJoins = false;

                if (t2.getSubject().isVariable() && joinVars.contains(t2.getSubject().toString())) {
                    subJoins = true;
                } else if (t2.getSubject().isVariable()) {
                    if (!joinFound)
                        joinVars.add(t2.getSubject().toString());
                } else {
                    if (!literalTriples.contains(t2))
                        literalTriples.add(t2);
                }

                if (t2.getObject().isVariable() && joinVars.contains(t2.getObject().toString())) {
                    objJoins = true;
                } else if (t2.getObject().isVariable()) {
                    if (!joinFound)
                        joinVars.add(t2.getObject().toString());
                } else {
                    if (!literalTriples.contains(t2))
                        literalTriples.add(t2);
                }

                if (!joinFound && (subJoins || objJoins)) {
                    joinedTriples.add(t2);
                    joinFound = true;
                }

                if (!subJoins && !objJoins) {
                    joinVars.remove(t2.getSubject().toString());
                    joinVars.remove(t2.getObject().toString());
                    literalTriples.remove(t2);
                }
            }

            if (joinedTriples.size() > 0) {
                for (Triple t : joinedTriples) { //Add first triple that joins (to avoid cross-joins by literal-triples)
                    if (!literalTriples.contains(t)) {
                        sortedTriples.remove(t);
                        reorderedTriples.add(t);
                        break;
                    }
                }
                if (literalTriples.size() > 0) {
                    for (int i = 0; i < literalTriples.size(); i++) { // first one is smallest (add to beginning of the list)
                        sortedTriples.remove(literalTriples.get(i));
                        boolean added = false;
                        for (int j = 0; j < reorderedTriples.size(); j++) {
                            if (reorderedTriples.get(j).getSubject().toString().equals(literalTriples.get(i).getSubject().toString()) ||
                                    reorderedTriples.get(j).getSubject().toString().equals(literalTriples.get(i).getObject().toString()) ||
                                    reorderedTriples.get(j).getObject().toString().equals(literalTriples.get(i).getSubject().toString()) ||
                                    reorderedTriples.get(j).getObject().toString().equals(literalTriples.get(i).getObject().toString())) {

                                if (j == 0) {
                                    reorderedTriples.add(0, literalTriples.get(i));
                                    added = true;
                                    break;
                                } else if ((j - 1 >= 0) && (reorderedTriples.get(j - 1).getSubject().equals(literalTriples.get(i).getSubject())
                                        || reorderedTriples.get(j - 1).getSubject().equals(literalTriples.get(i).getObject())
                                        || reorderedTriples.get(j - 1).getObject().equals(literalTriples.get(i).getSubject())
                                        || reorderedTriples.get(j - 1).getObject().equals(literalTriples.get(i).getObject()))) {
                                    reorderedTriples.add(j, literalTriples.get(i));
                                    added = true;
                                    break;
                                }
                            }
                        }
                        if (!added)
                            reorderedTriples.add(literalTriples.get(i));
                    }
                }
            } else {
                while (sortedTriples.size() > 0) {
                    reorderedTriples.add(sortedTriples.get(0));
                    sortedTriples.remove(0);
                }
            }
        }

        BasicPattern res = new BasicPattern();
        for (Triple t : reorderedTriples) {
            res.add(t);
        }

        return new OpBGP(res);
    }

    private ArrayList<String> sortByPropertySelectivity(ArrayList<String> candidates) {

        ArrayList<String> sortedMatches = new ArrayList<>();
        HashMap<String, String> propTableNames = new HashMap<>();
        HashMap propCount = new HashMap();

        for (String propWithID : candidates) {
//            boolean worqTable = false;
            int count = 0;
//            String propName = propWithID.split("\\_\\d+")[0];
//            String tableName = metadata.getTableName(propName);
//
//            if (!tableName.equals(propName)) {
//                propTableNames.put(propName, tableName);
//                worqTable = true;
//            }
//
//            if (worqTable) {
//                count = (int) metadata.getWORQTableInfo(tableName).get("numTuples");
//            } else {
//                count = (int) metadata.getVpTableInfo(tableName).get("numTuples");
//            }
            for (Map map : tablesStatistics) {
                if (map.get("tableName").equals(propWithID)) {
                    count = Integer.parseInt((String) map.get("numTuples"));
                    break;
                }
            }
            propCount.put(propWithID, count);
        }

        Map<String, Long> res = MapUtils.sortByValue(propCount, "asc");
        for (Map.Entry<String, Long> entry : res.entrySet()) {
            sortedMatches.add(entry.getKey());
        }
        return sortedMatches;
    }
}
