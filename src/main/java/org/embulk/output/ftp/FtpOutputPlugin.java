package org.embulk.output.ftp;

import com.google.common.base.Optional;
import com.google.common.base.Throwables;
import org.apache.commons.net.ftp.FTPClient;
import org.embulk.config.*;
import org.embulk.spi.Buffer;
import org.embulk.spi.Exec;
import org.embulk.spi.FileOutputPlugin;
import org.embulk.spi.TransactionalFileOutput;
import org.slf4j.Logger;

import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;

public class FtpOutputPlugin
        implements FileOutputPlugin
{
    private static final Logger log = Exec.getLogger(FtpOutputPlugin.class);

    public interface PluginTask
            extends Task
    {

        @Config("host")
        public String getHost();

        @Config("port")
        @ConfigDefault("null")
        public Optional<Integer> getPort();

        @Config("user")
        @ConfigDefault("null")
        public Optional<String> getUser();

        @Config("password")
        @ConfigDefault("null")
        public Optional<String> getPassword();

        //默认使用主动模式
       /* @Config("passive_mode")
        @ConfigDefault("true")
        public boolean getPassiveMode();*/

        @Config("ascii_mode")
        @ConfigDefault("false")
        public boolean getAsciiMode();

        @Config("path_prefix")
        public String getPathPrefix();

        @Config("file_ext")
        public String getFileNameExtension();

        @Config("sequence_format")
        @ConfigDefault("\"%03d.%02d.\"")
        public String getSequenceFormat();

    }

    //takes configuration from arguments and creates output tasks
    @Override
    public ConfigDiff transaction(ConfigSource config, int taskCount, Control control) {
        log.info("transaction called");
        PluginTask task = config.loadConfig(PluginTask.class);
        return resume(task.dump(), taskCount, control);
    }

    //called instead of "transaction" method if user resumes a failed transaction
    @Override
    public ConfigDiff resume(TaskSource taskSource, int taskCount, Control control) {
        log.info("resume called");
        control.run(taskSource);
        return Exec.newConfigDiff();
    }

    @Override
    public void cleanup(TaskSource taskSource, int taskCount, List<TaskReport> successTaskReports) {
        //todo
    }

    // takes one of the output tasks and creates a FileOutput instance.
    // FileOutput instance takes formatted data chunks and write them into files.
    @Override
    public TransactionalFileOutput open(TaskSource taskSource, int taskIndex) {
        log.info("open called");
        PluginTask task = taskSource.loadTask(PluginTask.class);
        return new FtpFileOutput(task,taskIndex);
    }

    private static FTPClient newFTPClient(Logger log, PluginTask task)
    {
        FTPClient client = new FTPClient();
        try {
            log.info("Connecting to "+task.getHost());
            if (task.getPort().isPresent()) {
                client.connect(task.getHost(), task.getPort().get());
            }

            if (task.getUser().isPresent()) {
                log.info("Logging in with user "+task.getUser().get()
                +client.login(task.getUser().get(), task.getPassword().or("")));

            }
            if (task.getAsciiMode()) {
                log.info("Using ASCII mode");
                client.setFileType(FTPClient.ASCII_FILE_TYPE);
            } else {
                log.info("Using binary mode");
                client.setFileType(FTPClient.BINARY_FILE_TYPE);
            }
            FTPClient connected = client;
            client = null;
            return connected;

        }  catch (IOException ex) {
            log.info("FTP network error: "+ex);
            throw Throwables.propagate(ex);

        } finally {
            if (client != null) {
                disconnectClient(client);
            }
        }
    }

    static void disconnectClient(FTPClient client)
    {
        if (client.isConnected()) {
            try {
                log.info("disconnect to ftp");
                client.disconnect();
            } catch (IOException ex) {
                // do nothing
            }
        }
    }

    public static class FtpFileOutput
            implements TransactionalFileOutput
    {
        private final List<String> fileNames = new ArrayList<>();
        private int fileIndex = 0;
        private OutputStream output = null;

        private String host;
        private int port;
        private String user;
        private String password;

        private String pathPrefix;
        private String pathSuffix;
        private String sequenceFormat;
        private int tIndex;
        private final Logger log = Exec.getLogger(FtpFileOutput.class);
        private FTPClient client;


        public FtpFileOutput(PluginTask task, int taskIndex) {
            host = task.getHost();
            port = task.getPort().get();
            user = task.getUser().get();
            password = task.getPassword().get();
            pathPrefix = task.getPathPrefix();
            pathSuffix = task.getFileNameExtension();
            sequenceFormat = task.getSequenceFormat();
            tIndex = taskIndex;
            client =  newFTPClient(log,task);
        }

        @Override
        public void nextFile() {
            closeFile();
            String path = pathPrefix + String.format(sequenceFormat, tIndex, fileIndex) + pathSuffix;
            log.info("Writing local file '{}'", path);
            fileNames.add(path);
            try {
                output = client.storeFileStream(path);
            } catch (Exception ex) {
                throw new RuntimeException(ex);  // TODO exception class
            }
            fileIndex++;

        }

        @Override
        public void add(Buffer buffer)
        {
            //log.info("add called");
            try {
                output.write(buffer.array(), buffer.offset(), buffer.limit());
            } catch (IOException ex) {
                throw new RuntimeException(ex);
            } finally {
                buffer.release();
            }

        }

        @Override
        public void finish() {
            log.info("finish called");
            closeFile();
            disconnectClient(client);
        }

        @Override
        public void close() {
            log.info("close called");
        }

        @Override
        public void abort() {
            log.info("abort called");
        }

        @Override
        public TaskReport commit() {
            TaskReport report = Exec.newTaskReport();
            return report;
        }

        private void closeFile()
        {
            log.info("closeFile called");
            if (output != null) {
                try {
                    //先关闭流,再调用completePendingCommand方法
                    //FTP服务器只有在接受流执行close方法时，才会返回226Transfer complete
                    output.close();
                    log.info("output stream closed");
                    if(!client.completePendingCommand()) {
                        log.info("completePendingCommand ,this should not show");
                        client.logout();
                        client.disconnect();
                    }
                } catch (IOException ex) {
                    throw new RuntimeException(ex);
                }
            }
        }
    }



}
