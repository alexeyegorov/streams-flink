package flink;

/**
 * @author alexey
 */
public abstract class StreamsFlinkObject {

    protected abstract void init() throws Exception;

    /**
     * Override readResolve method that is called every time an object has been deserialized.
     *
     * @return
     * @throws Exception
     */
    public Object readResolve() throws Exception {
        init();
        return this;
    }
}
