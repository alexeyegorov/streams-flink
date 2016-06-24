package flink.functions;

/**
 * Interface for all streams-flink function-objects (stream, process, queue, service).
 */
interface StreamsFlinkObjectInterface {

    /**
     * Init method has to be implemented in order to provide right behaviour after deserialization.
     */
    void init() throws Exception;

    /**
     * readResolve() is called every time an object has been deserialized. Inside of it init()
     * method is called in order to provide right behaviour after deserialization.
     *
     * @return this object
     */
    Object readResolve() throws Exception;
}
