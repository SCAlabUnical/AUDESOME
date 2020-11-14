## Audesome
Social media represents a rich environment to collect huge amount of data containing useful information on people's behaviors and interactions. This
information is particularly useful in the context of analyzing the mobility of people, where social media posts marked with geographic coordinates or other
information for identifying locations, allow to extract very precise rules on the mobility and movements of people. 

This paper presents **AUDESOME** (AUtomatic Detection of user trajEctories from SOcial MEdia), an automatic method aimed at discovering user mobility patterns from social media posts. In
particular, we have dened two new unsupervised algorithms: 
* A text mining algorithm that analyzes the content of posts to automatically extract the main keywords identifying the Places of Interest (PoI) present in a given area; 
* a clustering algorithm that detects the Regions of Interest (RoIs) starting from the extracted keywords and geotagged posts of users. We experimentally evaluated the accuracy of AUDESOME taking into account following aspects: i) extraction
the keywords identifying the PoIs; ii) detection of the RoIs;
* mining of user trajectories. 

The experiments, performed on a real datasets containing about 3.1 millions of geotagged items published in Flickr in the areas of Rome and Paris, demonstrate that AUDESOME achieves better results than existing techniques.

## How to use Audesome
To use this project you must install sbt. From repository directory open a shell and type:
```shell script
sbt compile
```
```shell script
sbt run <commands_argument>
```
Command arguments are:

```
      --dataset-path  <arg>           Path to input dataset.
      --debug-level                   Log level.
  -d, --driver-memory  <arg>          Amount of memory reserved for driver
                                      program application
  -e, --executor-memory  <arg>        Amount of memory reserved for Spark
                                      executor
  -k, --keywords-path  <arg>          Path to keyword's file.
  -l, --limits  <arg>                 Limit output.
  -n, --number-of-partitions  <arg>   Number of partitions used for Dataframe
  -r, --rois-path  <arg>              Path to computed roi's file.
      --spark-application  <arg>      Name of current spark application
  -s, --spark-hostname  <arg>         Endpoint of a spark cluster (empty if you
                                      want to use a local cluster)
      --stop-words  <arg>             Path to stop word's file.
  -t, --threads-count  <arg>          Number of threads for spark driver in
                                      local execution
  -h, --help                          Show help message
```

For example to start audesome with 2 threads: 
```shell script
sbt run --threads-count 2
```

### AUDESOME with 2 threads and 50 partitions
```shell script
sbt run --threads-count 2 --number-of-partitions 50
```

### AUDESOME with 2 threads and 50 partitions and custom memory
```shell script
sbt run --threads-count 2 --number-of-partitions 50 -driver-memory 1Gi --executor-memory 2Gi
```

### AUDESOME change data path
```shell script
sbt run --dataset-path /path/to/input --keywords-path /path/to/keywords
```

## Docker support
In order to use the source code we provide a Dockerfile definition with a pratical example. **To use container you must have docker runtime environment**.
To use container open a shell and type:

```shell script
sh ./run.sh
```
