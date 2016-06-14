# Streams Flink #

This project tries to slightly modify [streams-storm](https://bitbucket.org/cbockermann/streams-storm) project in order to adapt it to ``Flink``. This way we can achieve parsing of XML configuration files for ``streams framework`` and translating them into ``Flink`` topology.

The XML definition of ``streams`` process has not been changed.
We can still use ``copies`` attribute in ``<process ...>`` tag in order to controll the level of parallelism.
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

The easiest way to start a Flink job is to use the submit script by Flink itself:

```
./bin/flink run --jobmanager jobmanager-address:6123 -p <parallelism-level> <jar-file> <further-arguments>
```