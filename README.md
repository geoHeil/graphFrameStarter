# getting started with graph-frames
Mini project to get me started with graph-frames.
I want to compute several metrics for each node.

For each node compute percentage of fraudulent connections for 
  - direct node (directed)
  - direct node (undirected)
  - the friendship network form the node (directed)
  - the friendship network form the node (undirected)
in total and per connection type.

Getting started with graph-frames I am not sure how to move forward. Looking forward to some suggestions.

The following nodes are present:

```
+---+-------+-----+
| id|   name|fraud|
+---+-------+-----+
|  a|  Alice|    1|
|  b|    Bob|    0|
|  c|Charlie|    0|
|  d|  David|    0|
|  e| Esther|    0|
|  f|  Fanny|    0|
|  g|  Gabby|    0|
+---+-------+-----+
```

and edges
```
+---+---+------------+
|src|dst|relationship|
+---+---+------------+
|  a|  b|           A|
|  b|  c|           B|
|  c|  b|           B|
|  f|  c|           B|
|  e|  f|           B|
|  e|  d|           A|
|  d|  a|           A|
|  a|  e|           A|
+---+---+------------+
```
