# Streams Flink #

This project tries to slightly modify [streams-storm](https://bitbucket.org/cbockermann/streams-storm) project in order to adapt it to ``Flink``. This way we can achieve parsing of XML configuration files for ``streams framework`` and translating them into ``storm`` topology which is then used by ``flink-storm`` compatibility layer to build its own topology.

**Important:** still in work and not fully working as ``flink-storm`` is also BETA feature of ``Flink`` project.