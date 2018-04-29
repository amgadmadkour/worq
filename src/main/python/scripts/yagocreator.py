import sys


def identifyprefix(uri):
    prefixList = {"http://yago-knowledge.org/resource/": "yago",
                  "http://dbpedia.org/ontology/": "dbp",
                  "http://www.w3.org/2002/07/owl#": "owl",
                  "http://www.w3.org/1999/02/22-rdf-syntax-ns#": "rdf",
                  "http://www.w3.org/2000/01/rdf-schema#": "rdfs",
                  "http://www.w3.org/2004/02/skos/core#": "skos",
                  "http://www.w3.org/2001/XMLSchema#": "xsd",
                  "http://dbpedia.org/class/yago/": "yagoc",
                  "http://dbpedia.org/resource/": "dbr"}

    for key in prefixList:
        if key in uri:
            return uri.replace(key, prefixList[key] + "__")
    return uri


if __name__ == '__main__':

    for line in sys.stdin:
        if line.startswith("#") or line.startswith("@"):
            continue
        line = line.strip()
        line = line.replace(" .", "")
        parts = line.split("\t")
        newparts = list()
        for p in parts:
            if p.startswith("<") and p.endswith(">") and "http://" in p:
                p = p[1:len(p) - 1];
                newparts.append(identifyprefix(p))
            elif p.startswith("<") and p.endswith(">") and "http://" not in p:
                p = p[1:len(p) - 1];
                newparts.append("yago__" + p)
            elif not p.startswith("<") and not p.endswith(">") and ":" in p:
                parts = p.split(":")
                newparts.append(parts[0] + "__" + parts[1])
            else:
                newparts.append(p)

        if len(newparts) == 3:
            line = " ".join(newparts) + " ."
            print line
