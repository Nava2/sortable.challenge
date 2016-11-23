# Sortable Challenge

Written by Kevin Brightwell (@Nava2).

This is an implementation of the [Sortable Coding Challenge](http://sortable.com/challenge/).
It is implemented using [Apache Spark](http://spark.apache.org/) and several other open source 
libraries primarily for convenience.

This implementation is far from perfect, but favours valid less correct results over false 
positives, as per the challenge definition. The implementation uses a combination of NGrams 
and Levishtein distance computations to attempt to match the listings to products against a 
simple cost function and weighing scheme. For example, if a title of a listing contains the 
manufacturer AND the model then it is weighted stronger. 

## Running Instructions

### Prerequisites

1. [sbt](http://www.scala-sbt.org/) is installed and available in the path

### Running

To run the application, from a console: 

To get command-line options:
```bash
sbt "run -h"
```

To run locally on four threads, writing to `result.jsonlines`
```bash
sbt "run --localThreads 4 -l ./listings.txt -p products.txt -o result.jsonlines"
```

Running on 4 cores takes ~50s. 