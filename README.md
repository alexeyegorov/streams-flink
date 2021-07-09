# Streams Flink [![CodeQL](https://github.com/alexeyegorov/streams-flink/actions/workflows/codeql-analysis.yml/badge.svg)](https://github.com/alexeyegorov/streams-flink/actions/workflows/codeql-analysis.yml)

This project tries to slightly modify [streams-storm](https://bitbucket.org/cbockermann/streams-storm) project in order to adapt it to Apache Flink (ver. 1.3.2). 
This way we can achieve parsing of XML configuration files for ``streams framework`` and translating them into ``Flink`` topology.

The XML definition of ``streams`` process has not been changed.
We can still use ``copies`` attribute in ``<process ...>`` tag in order to control the level of parallelism.
Each copy is then mapped to a task slot inside of the Flink cluster.
We have support for ``services`` and ``queues``. 
Each ``process``, e.g. as the following
```
<process input="data" id="extraction" copies="1">
```

is translated to a flatMap function with parallelism and name set over ``copies`` and ``id``:

```
DataStream<Data> dataStream = source
		.flatMap(function)
        .setParallelism(Integer.parseInt(element.getAttribute("copies")))
        .name(element.getAttribute("id"));
```

While using another process (e.g. reading from a second source or a queue) tasks may be mapped onto the same physical machine.
Hence, e.g. if we have two machines with 6 cores each (task slots) and have two subprocesses with the level of parallelism set to 6 each, they might both run on the same task slots on one single machine. 

## Getting started

After checking out the repository all you need to do is build the approriate JAR file with the following command:

```
mvn -P standalone package
```

Then with the following commands you can add new or stop any old jobmanager or taskmanager:

```
{flink-folder}/bin/jobmanager.sh ((start|start-foreground) cluster)|stop|stop-all
{flink-folder}/bin/taskmanager.sh start|start-foreground|stop|stop-all
```

This can also be done by adjusting the configuration file and calling:

```
{flink-folder}/bin/start-cluster.sh
{flink-folder}/bin/stop-cluster.sh
```

If you are not familiar with Flink's configuration, see its [documentation](https://ci.apache.org/projects/flink/flink-docs-release-1.3/setup/cluster_setup.html).

The easiest way to start a Flink job is to use the submit script by Flink itself:

```
{flink-folder}/bin/flink run --jobmanager <jobmanager-address>:6123 -p <parallelism-level> <jar-file> <further-arguments>
```

The parallelism level defines the default which is being used if the XML file does not provide any information of the parallelism.
If the jobmanager is not the local machine, then the JAR file will be transmitted to it and it might take some time due to its size.
`further-arguments` might be a path to XML file.

Using the web GUI under `<jobmanager-address>:8081` you can now watch the details of the running jobs.

## Flink on YARN

Support for HDFS / YARN requires the following variables to be set:

```
export HADOOP_HOME='/path/to/hadoop/'
export HADOOP_USER_NAME='username'
export YARN_CONF_DIR='/path/to/conf/'
export HADOOP_CONF_DIR='/path/to/conf/'
```

Then using Flink submit script we can start a yarn session:

```
./bin/yarn-session.sh -n 20 -s 1 -jm 4096 -tm 8192 -d
```

Each session receives a unique id which has to be used to submit jobs to the existing yarn session as following:

```
./bin/flink run -m yarn-cluster -yid application_1481114932164_0118 streams-flink-{version}-flink-compiled.jar hdfs://path/to/streams/jobdefinition/on/hdfs.xml
```
