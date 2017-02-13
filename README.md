# WikiData to Neo4J

This is a little script that reads the json dumps from WikiData and outputs a file
containing Cypher statements (the language used by Neo4J) which will insert nodes
and edges into a graph.

This is a very alpha version, not really tested -- use at your own risk, your mileage
may vary, etc.

Run `python read_wikidata.py -h` for usage information.

