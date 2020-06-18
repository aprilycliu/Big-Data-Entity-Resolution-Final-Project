

# Big Data Entity Resolution 

### Song Xingchang, Yuchun Liu
### 1. Project Objective
The report will demonstrate the steps of the implementation of algorithms in the entity resolution process. First, Token Blocking and Attribute Clustering Blocking method will be applied to cluster the data collections into blocks. Second, Meta-Blocking method using as edge weighting the Jaccard scheme and the common blocks scheme, and as pruning scheme the weight edge pruning and the cardinality node pruning methods will be implemented to discard unnecessary comparisons.
### 2. Dataset
The movie dataset is used for the implementation. Each movie presents as one entity with 7 attributes after preprocessing. There are 150 entities in each of the two data collections with 90 of them as duplicates. The datasets are stored in csv format file.
### 3. Preprocessing
After extraction of 150 entities from each collections, we renamed of the column names and randomly reset them in irregular shapes so that different entities contain different attribute-value pairs. In such way, the experiment entities are arranged similarly to real-world ones. In the implementation, the two entity sets are represented in two dataframe so that each row in a dataframe indicates an entity and each column represents a attribute-value-pair set.
Collection 1| Collection 2
------------ | -------------
movie_name (150) | titles (150)
language (100) | original_language (88)
box_office (98) | popularity (104)
year (98) | release_year (98)
companies (101) | production_companies (94) 
countries (106) | production_countries (115) 
director (91) | director (106)
### 4. Token Blocking
Token Blocking is based on the following idea: every distinct token 𝑡𝑖 creates a separate block 𝑏𝑖 that contains all entities having 𝑡𝑖 in the values of their profile — regardless of the associated attribute names. In implementation, a mapreduce method is used to place different entities into different blocks representing different tokens. More specifically, token-entity pairs are produced during mapping stage while blocks are collected and stored for later usage in reducing phase.
### 5. Attribute Clustering
As for attribute clustering, at its core lies the idea of partitioning attribute names, according to the similarity of their values, into non-overlapping clusters, in which entities having the same attribute value in an attribute are placed. In implementation, the process is divided into two part. First, similar attributes from two entity ests are found by comparing each column from one entity set with all columns from another. Second, by using the information gained in the f
irst step, a mapreduce method is applied to produce attribute-value-entity pairs so that the key is attribute values within similar attribute columns and the value is the entities.
### 6. Metabocking
In order to achieve the best balance between precision and recall, Meta-blocking is implemented. We restructured the block collection from Token Blocking and Attribute Clustering into new collection which contains a lower number of comparisons. The first part of Meta-Blocking is to assign edge weights, with two different scheme being applied: Jaccard scheme and the common blocks scheme. The edges with low weights correspond to superfluous comparisons and therefore are pruned in the next step. The second part is to apply pruning algorithm with two difference scheme: weight edge pruning and the cardinality node pruning methods.
### 6.1 Jaccard scheme
The similarity of two entities can be measured by jaccard scheme. At its core lies the idea that the similarity of two entities is estimated by the portion of blocks shared by two entities.
JS(𝑒𝑖,𝑒𝑗) = |Bi,j| / |Bi|+|Bj|−|Bi,j|
### 6.2 Common blocks scheme
Considering the similarity of two entities is provided by the number of blocks they have in common, a Data Frame is created with every pairs of comparison and the corresponding number of occurence from block collections. The number of occurence indicate the weight, and is sorted in descending order.
### 6.3 Weighted Edge Pruning method
Weighted Edge Pruning(WEP) comprises an edge-centric pruning algorithm that uses a global weight threshold.In essence,it discards all edges that do not exceed the average edge weight of the entire blocking graph. In the experiment, meta-blocking using jaccard shceme reduces the number of comparisons to 13,758 and 13,765 after applying token blocking and attribute clustering, respectively. It can be seen that WEP reduces 60% or so comparisons in the pruning step.
Block Collection | Initial Comparisons (Jaccard Scheme) | After WEP pruning, distinct comparsions | Pairs Quality (PQ) | Pairs Completeness (PC) 
------------ | ------------- | ------------- | ------------- | ------------- 
Token Blocking |13758 |5011 |0.0188| 0.8556 |
Attribute Clustering |13695 |5042 | 0.0186| 0.8556
### 6.4 Cardinality node pruning method
CNP consists of a node-centric pruning algorithm that uses a global cardinality threshold. For each node, it retains the top-k edges of its neighborhood, where k is defined as: k = BC(B) − 1, where BC(B) stands for the Blocking Cardinality of B, i.e., the average number of blocks associated with every entity in B. After calculation, k of two block collections are (1133, 1152, respectively to blocks from Token Blocking and Attribute Clustering Method. After knowing the k as threshold, the cumulative sum of the number of pair occurence is computed and with sum less than threshold is retained.
Block Collection | Initial Comparisons (Common Block) | k | After CNP pruning, distinct comparsions | Pairs Quality (PQ) | Pairs Completeness (PC) 
------------ | ------------- | ------------- | ------------- | ------------- | ------------- 
Token Blocking | 21254 | 1133 | 168 | 0.465 | 0.822 
Attribute Clustering | 20184 | 1152 | 172 | 0.458 | 0.788

### 7. Evalution
After Meta-Blocking Method, the smaller block collections are created. With the block Index, the properties of each entities will be extracted. Later, we compute token-based Jaccard similarity between properties to evaluate the matching result. Finally, we compute the Pairs Quality, expressing the portion of comparisons that correspond to a non-redundant pair of duplicates, and Pairs Completeness, expressing the portion of existing pairs of duplicates that can be detected in B.
### 8. Conlusion
The implementation of task 1 and 2 is achieved by obtaining the desired results. A novel way to represent entity in dataset is provided by using dataframe in python. Some improvements could be made in terms of integrating task 1 and 2.
