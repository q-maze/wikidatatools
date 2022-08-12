# wikidatatools
Tools for preprocessing Wikidata for knowledge graph embeddings.

Wikidata tools is a set of tools for efficiently preprocessing Wikidata n-triples dumps into formats that can be digested by knowledge graph embedding (KGE) libraries such as PyKeen.

We provide two files:
* processTruthy.py - Set of tools for processing a Wikidata n-triples dump from its .nt.bz2 format into .tsv files. 
To run this file, simply navigate to the directory that it is stored and run the command `python processTruthy.py -f path/to/ntriplesdump.nt.bz2`. 
You may also add the `-o` flag to specify an output directory and the `-c` flag to specify a chunksize in lines to pass to each worker.
This will create four .tsv files in the current directory if `-o` is not specified, or in the directory of your choice:
  * `edges.tsv` - Entity-Property-Entity triples of the form head/relation/tail detailing relationships between items
  * `meta.tsv` - Entity-Label-Value triples containing labels of each Wikidata item
  * `data.tsv` - Entity-Property-Value triples containing data about items
  * `errors.tsv` - Output file for any errors encountered during decoding
* subsetTools.py - Set of tools for processing `edges.tsv` file created using `processTruthy.py` into smaller subsets. 
The script first queries Wikidata's SPARQL endpoint to retrieve a list $C$ of all subclasses of a user defined class. 
This list is then used to create a filter for the Dask dataframe $I = (h, r_{instance}, c) \forall c \in C$, 
where $r_{instance}$ is a special property, P31, indicating the head entity $h$ is an instance of class $c$. 
The instance list $I$ is then used to create the subset $S = (h,r,t) \forall (h,t) \in I$. To run this script, simply
run the `create_subset(targets, input_path, output_path)` function specifying a target Wikidata class(es) in the form 'Q123'
as a string or iterable of strings. 

As this set of tools was created as a pipleine to supply data to a [PyKEEN](https://github.com/pykeen/pykeen) model
as part of our capstone project at the University of Virginia, we include an example model training script 
in the examples folder. We also include the datasets from our paper [**Review of Knowledge Graph Embedding Models
for Link Prediction on Wikidata Subsets**](https://github.com/q-maze/wikidatatools/blob/main/WikidataKGEReview.pdf) as examples.
