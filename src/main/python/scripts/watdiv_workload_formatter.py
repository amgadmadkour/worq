# Input 1: Original file
# Input 2: Output directory
# Input 3: ID file
# Output: The output directory would contain the queries that qualify the test

from itertools import izip
import sys

if __name__ == '__main__':

    output_dir = sys.argv[2]

    with open(sys.argv[1]) as textfile1, open(sys.argv[3]) as textfile2:
        for x, y in izip(textfile1, textfile2):
            # Scan the query for the variables and replace the asterix with the variables
            line = x.strip()
            id = y.strip()
            variable_list = set()

            parts = line.split(" ")
            for token in parts:
                if token.startswith("?"):
                    variable_list.add(token)

            line = line.replace("*", " ".join(variable_list))

            line = """PREFIX dc: <http://purl.org/dc/terms/>
                      PREFIX foaf:	<http://xmlns.com/foaf/>
                      PREFIX gr:	<http://purl.org/goodrelations/>
                      PREFIX gn:	<http://www.geonames.org/ontology#>
                      PREFIX mo:	<http://purl.org/ontology/mo/>
                      PREFIX og:	<http://ogp.me/ns#>
                      PREFIX rev:	<http://purl.org/stuff/rev#>
                      PREFIX rdf:	<http://www.w3.org/1999/02/22-rdf-syntax-ns#>
                      PREFIX rdfs:	<http://www.w3.org/2000/01/rdf-schema#>
                      PREFIX sorg:	<http://schema.org/>
                      PREFIX wsdbm:	<http://db.uwaterloo.ca/~galuc/wsdbm/>\n\n""" + line

            output_file = open(output_dir + "/{}".format(id) + "__VP_SO-OS-SS-VP.in", 'w')
            output_file.write(line + '\n')
            output_file.close()
