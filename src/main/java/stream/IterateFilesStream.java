package stream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import stream.annotations.Parameter;
import stream.io.AbstractStream;
import stream.io.SourceURL;
import stream.io.multi.AbstractMultiStream;

/**
 * @author alexey
 */
public class IterateFilesStream extends AbstractMultiStream {

    static Logger log = LoggerFactory.getLogger(IterateFilesStream.class);

    @Parameter(required = true)
    String ending = "";

    @Parameter(required = false, defaultValue = "true",
            description = "If something goes wrong while reading a file, just continue with the next one.")
    boolean skipErrors = true;

    private FileStatus[] fileStatuses;
    private int fileCounter;

    private AbstractStream stream;
    private int failedFilesCounter;
    private ArrayList<String> failedFilesList;
    private int countReadNext;

    public IterateFilesStream() {
        super();
    }

    /**
     * Filter files in HDFS folder.
     */
    private class HDFSPathFilter extends Configured implements PathFilter {
        Configuration conf;
        Pattern pattern;

        @Override
        public boolean accept(Path path) {
            Matcher matcher = pattern.matcher(path.toString());
            return matcher.matches();
        }

        @Override
        public void setConf(Configuration conf) {
            this.conf = conf;
            pattern = Pattern.compile("^.*" + ending + "$");
        }
    }

    @Override
    public void init() throws Exception {

        Configuration config = new Configuration();
        FileSystem fs = FileSystem.get(new URI(this.url.toString()), config);

        correctWorkingDirectory(fs);

        String path = this.url.getPath();
        if (!path.endsWith("/")) {
            path += "/";
        }
        fileStatuses = fs.listStatus(new Path(path), new HDFSPathFilter());
        log.info("Found {} files in the HDFS folder.", fileStatuses.length);
        fileCounter = 0;
        countReadNext = 0;
        if (stream == null && additionOrder != null) {
            stream = (AbstractStream) streams.get(additionOrder.get(0));
            stream.setUrl(new SourceURL(fileStatuses[fileCounter].getPath().toString()));
            stream.init();
            log.info("Streaming file {}: {}", fileCounter, stream.getUrl().toString());
            fileCounter++;
        }
    }

    /**
     * Change working directory that is expanded by default with "/user/'username'/". If this is the
     * case, than set working directory to a new value using protocol, host and port of a URL.
     *
     * @param fs filesystem
     */
    private void correctWorkingDirectory(FileSystem fs) {
        if (!url.toString().startsWith(fs.getWorkingDirectory().toString())) {
            String host = this.url.getHost();
            int port = this.url.getPort();
            String protocol = this.url.getProtocol();
            String workingDirectory = protocol + "://" + host + ":" + port + "/";
            log.info("\nGiven URL is {}.\n But working directory is set to {}.\nChanging it to {}",
                    url.toString(), fs.getWorkingDirectory(), workingDirectory);
            fs.setWorkingDirectory(new Path(workingDirectory));
        }
    }

    @Override
    public void close() throws Exception {
        super.close();

        // log all skipped files
        if (failedFilesCounter > 0) {
            String failesFiles = "";
            for (String failedFile : failedFilesList) {
                failesFiles += "\n" + failedFile;
            }
            log.info("Some files has been skipped because of errors: {}", failesFiles);
        }
    }

    @Override
    public Data readNext() throws Exception {

        try {
            Data data = stream.read();
            if (data == null) {
                //get new file
                stream.close();
                //no data was returned
                if (fileStatuses.length <= fileCounter) {
                    //no more files to read -> stop the stream
                    return null;
                }

                stream.setUrl(new SourceURL(fileStatuses[fileCounter].getPath().toString()));
                stream.init();

                log.info("Streaming file {}: {}", fileCounter, stream.getUrl().toString());

                fileCounter++;
                data = stream.readNext();
            }

            log.info("Read {} items", countReadNext++);
            return data;

        } catch (IOException e) {
            log.info("File: " + stream.getUrl().toString() + " throws IOException.");

            if (skipErrors) {
                log.info("Skipping broken files. Continuing with next file.");
                stream = null;
                failedFilesCounter++;
                failedFilesList.add(stream.getUrl().toString());
                return this.readNext();
            } else {
                log.error("Stopping stream because of IOException");
                e.printStackTrace();
                stream.close();
                return null;
            }
        }
    }
}
