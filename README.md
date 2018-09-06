# Workload-Driven RDF Query Processing (WORQ)

### About

WORQ computes reduced sets of intermediate results (or reductions, for short) that are common for certain join pattern(s). Furthermore, these reductions are not computed beforehand, but are rather computed only for the frequent join patterns in an online fashion using Bloom filters. In addition, WORQ caches the reductions in contrast to caching the final results of each query. WORQ does not assume prior knowledge of the workload. Instead, as the queries are being processed, all join patterns are determined, and accordingly, the partitioning of the data as well as the main-memory cached reductions are dynamically updated. In addition, WORQ provides an efficient solution for RDF queries with unbound properties. This is 

### Usage

The parameters for each of the following scripts can be modified from within the python scripts.

* **runLoader**: Creates an RDF store
* **runBloom**: Creates the Bloom filter separately. This step is also pre-included in the runLoader
* **runExecutor**: Runs the workload queries

Please refer to our more recent implementation: **[Knowledge Cubes](https://github.com/amgadmadkour/knowledgecubes)**