package uk.ac.ebi.age.loader;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.PrintStream;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.HttpStatus;
import org.apache.http.NameValuePair;
import org.apache.http.client.HttpClient;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.message.BasicNameValuePair;
import org.apache.http.protocol.HTTP;
import org.apache.http.util.EntityUtils;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;

import uk.ac.ebi.age.admin.shared.Constants;
import uk.ac.ebi.age.admin.shared.MaintenanceModeConstants;

public class ATLoader {
    static final String usage = "java -jar ATLoader.jar -o OUTDIR [options...] <input dir> [ ... <input dir> ]";

    private static Options options = new Options();

    public static void main(String[] args) {

        CmdLineParser parser = new CmdLineParser(options);

        try {
            parser.parseArgument(args);
        } catch (CmdLineException e) {
            System.err.println(e.getMessage());
            System.err.println(usage);
            parser.printUsage(System.err);
            System.exit(1);
            return;
        }

        if (options.getDirs() == null || options.getDirs().size() == 0) {
            System.err.println(usage);
            parser.printUsage(System.err);
            System.exit(1);
            return;
        }

        Set<File> indirs = new HashSet<File>();

        Set<String> processedDirs = new HashSet<String>();

        for (String outf : options.getDirs()) {
            File in = new File(outf);

            if (!in.exists()) {
                System.err.println("Input directory '" + outf + "' doesn't exist");
                System.exit(1);
                return;
            } else if (!in.isDirectory()) {
                System.err.println("'" + outf + "' is not a directory");
                System.exit(1);
                return;
            }

            indirs.add(in);
        }

        if (indirs.size() == 0) {
            System.err.println("No files to process");
            System.exit(1);
            return;
        }

        if (options.getOutDir() == null) {
            System.err.println("Output directory is not specified");
            System.exit(1);
            return;
        }

        File outDir = new File(options.getOutDir());

        if (outDir.isFile()) {
            System.err.println("Output path must point to a directory");
            System.exit(1);
            return;
        }

        if (!outDir.exists() && !outDir.mkdirs()) {
            System.err.println("Can't create output directory");
            System.exit(1);
            return;
        }

        int nThreads = -1;

        if (options.getThreadsNumber() != null) {
            try {
                nThreads = Integer.parseInt(options.getThreadsNumber());
            } catch (Exception e) {
                // Do nothing
            }

            if (nThreads <= 0 || nThreads > 32) {
                System.err.println("Invalid number of threads. Should be reasonable positive integer");
                System.exit(1);
                return;
            }
        } else
            nThreads = 1;

        BlockingQueue<File> infiles = new LinkedBlockingQueue<File>();

        PrintStream log = null;
        try {
         log = new PrintStream(new File(outDir, "log.txt"));
        } catch (FileNotFoundException e1) {
         System.err.println("Can't create log file: " + new File(outDir, "log.txt").getAbsolutePath());
         System.exit(1);
         return;
        }

        if (options.isPreloadFiles() || nThreads == 1) {
         Log psLog = new PrintStreamLog(log, true, false);
 
         new CollectFilesTask(indirs, infiles, options, psLog).run();

            if (infiles.size() <= 1) {
                System.err.println("No files to process");
                System.exit(1);
                return;
            }

            System.out.println(String.valueOf(infiles.size() + " submissions in the queue"));
        }

        String sessionKey = null;
        DefaultHttpClient httpclient = null;



        if (options.getDatabaseURL() == null) {
            System.err.println("Database URI is required for remote operations");
            System.exit(1);
            return;
        } else if (!options.getDatabaseURL().endsWith("/"))
            options.setDatabaseURI(options.getDatabaseURL() + "/");

        if (options.getUser() == null) {
            System.err.println("User name is required for remote operations");
            System.exit(1);
            return;
        }

        boolean ok = false;

        try {

            httpclient = new DefaultHttpClient();
            HttpPost httpost = new HttpPost(options.getDatabaseURL() + "Login");

            List<NameValuePair> nvps = new ArrayList<NameValuePair>();
            nvps.add(new BasicNameValuePair("username", options.getUser()));
            nvps.add(new BasicNameValuePair("password", options.getPassword() != null ? options.getPassword() : ""));

            httpost.setEntity(new UrlEncodedFormEntity(nvps, HTTP.UTF_8));

            log.println("Trying to login onto the server");

            HttpResponse response = httpclient.execute(httpost);

            if (response.getStatusLine().getStatusCode() != HttpStatus.SC_OK) {
                log.println("Server response code is: " + response.getStatusLine().getStatusCode());
                System.exit(2);
                return;
            }

            HttpEntity ent = response.getEntity();

            String respStr = EntityUtils.toString(ent).trim();

            if (respStr.startsWith("OK:")) {
                System.out.println("Login successful");
                log.println("Login successful");
                sessionKey = respStr.substring(3);
            } else {
                log.println("Login failed: " + respStr);
                System.exit(3);
                return;
            }

            EntityUtils.consume(ent);

            ok = true;
        } catch (Exception e) {
            log.println("ERROR: Login failed: " + e.getMessage());
            log.close();
            System.exit(3);
            return;
        } finally {
            if (!ok) {
                httpclient.getConnectionManager().shutdown();
                System.err.println("Login failed");
                System.exit(3);
                return;
            }
        }

        if (options.isMaintenanceMode())
            setMaintenanceMode(httpclient, true, sessionKey, log);

        if (nThreads == 1) {
            Log psLog = new PrintStreamLog(log, true, false);

            int status = new SubmitterTask("Main", options.getDatabaseURL() + "upload?" + Constants.sessionKey + "="
                    + sessionKey, infiles, outDir, options, psLog).call();

            psLog.shutdown();

            if (status != SubmitterTask.ST_OK)
                System.exit(10);
        } else {
            Log psLog = new PrintStreamLog(log, true, true);

            psLog.write("Starting " + nThreads + " threads");

            ExecutorService exec = Executors.newFixedThreadPool(nThreads + 1);

            if( ! options.isPreloadFiles() )
             exec.execute(new CollectFilesTask(indirs, infiles, options, psLog ));

            @SuppressWarnings("unchecked")
            Future<Integer> results[] = new Future[nThreads];

            for (int i = 1; i <= nThreads; i++)
                results[i - 1] = exec.submit(new SubmitterTask("Thr" + i, options.getDatabaseURL() + "upload?"
                        + Constants.sessionKey + "=" + sessionKey, infiles, outDir, options, psLog));

            int status = 0;

            try {

                for (int i = 0; i < nThreads; i++) {
                    try {
                        if ((results[i].get() != SubmitterTask.ST_OK) && status < 10)
                            status = 10;
                    } catch (ExecutionException e) {
                        status = 11;
                    }
                }
                exec.shutdown();

                exec.awaitTermination(72, TimeUnit.HOURS);
            } catch (InterruptedException e) {
            }

            if (options.isMaintenanceMode())
                setMaintenanceMode(httpclient, false, sessionKey, log);

            psLog.shutdown();

            System.exit(status);
        }

    }

    private static void setMaintenanceMode(HttpClient httpclient, boolean mode, String sessionKey, PrintStream log) {
        boolean ok = false;

        try {

            HttpPost httpost = new HttpPost(options.getDatabaseURL() + "upload?" + Constants.sessionKey + "="
                    + sessionKey);

            List<NameValuePair> nvps = new ArrayList<NameValuePair>();
            nvps.add(new BasicNameValuePair(Constants.serviceHandlerParameter, Constants.MAINTENANCE_MODE_COMMAND));
            nvps.add(new BasicNameValuePair(MaintenanceModeConstants.modeParam, String.valueOf(mode)));

            httpost.setEntity(new UrlEncodedFormEntity(nvps, HTTP.UTF_8));

            log.println((mode ? "Entering" : "Leaving") + " maintenance mode");

            HttpResponse response = httpclient.execute(httpost);

            if (response.getStatusLine().getStatusCode() != HttpStatus.SC_OK) {
                log.println("Server response code is: " + response.getStatusLine().getStatusCode());
                return;
            }

            HttpEntity ent = response.getEntity();

            String respStr = EntityUtils.toString(ent).trim();

            if (respStr.startsWith("OK:")) {
                if ("WAS".equals(respStr.substring(3, 6))) {
                    System.out.println("Server was in requested mode");
                    log.println("Server was in requested mode");
                } else {
                    System.out.println((mode ? "Entering" : "Leaving") + " maintenance mode successful");
                    log.println((mode ? "Entering" : "Leaving") + " maintenance mode successful");
                }

                ok = true;
            } else if (respStr.startsWith("ERROR:"))
                log.println("Maintenance mode switch error: " + respStr.substring(6));

            EntityUtils.consume(ent);

        } catch (Exception e) {
            ok = false;
        } finally {
            if (!ok) {
                httpclient.getConnectionManager().shutdown();
                System.err.println("Setting maintenance mode failed");

                System.exit(20);
            }
        }

    }

    static class NullLog implements Log {
        @Override
        public void shutdown() {
        }

        @Override
        public void write(String msg) {
        }

        @Override
        public void printStackTrace(Exception e) {
        }
    }

    static class PrintStreamLog implements Log {
        private PrintStream log;
        private Lock lock = new ReentrantLock();

        private static DateFormat dformat = new SimpleDateFormat("dd/MM/yy HH:mm:ss");
        
        private boolean showThreads;
        private boolean showTime;

        PrintStreamLog(PrintStream l, boolean tm, boolean th) {
            log = l;
            showThreads = th;
            showTime = tm;
        }

        public void shutdown() {
            log.close();
        }

        public void write(String msg) {
            lock.lock();

            try {
             if (showTime)
              log.print(dformat.format( new Date() ));

             if (showThreads)
                    log.print("[" + Thread.currentThread().getName() + "] ");

                log.println(msg);
            } finally {
                lock.unlock();
            }
        }

        @Override
        public void printStackTrace(Exception e) {
            lock.lock();

            try {
                e.printStackTrace(log);
            } finally {
                lock.unlock();
            }
        }

    }

}
