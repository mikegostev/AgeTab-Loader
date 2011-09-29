package uk.ac.ebi.age.loader;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.HttpStatus;
import org.apache.http.NameValuePair;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.message.BasicNameValuePair;
import org.apache.http.protocol.HTTP;
import org.apache.http.util.EntityUtils;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;

import uk.ac.ebi.age.admin.shared.Constants;


public class ATLoader
{
 static final String usage = "java -jar ATLoader.jar -o OUTDIR [options...] <input dir> [ ... <input dir> ]";


 private static Options options = new Options();
 
 public static void main(String[] args)
 {
  
  CmdLineParser parser = new CmdLineParser(options);

  try
  {
   parser.parseArgument(args);
  }
  catch(CmdLineException e)
  {
   System.err.println(e.getMessage());
   System.err.println(usage);
   parser.printUsage(System.err);
   return;
  }
  
  if( options.getDirs() == null || options.getDirs().size() == 0  )
  {
   System.err.println(usage);
   parser.printUsage(System.err);
   return;
  }
  
  
  Set<File> indirs = new HashSet<File>();

  Set<String> processedDirs = new HashSet<String>();
  
  for(String outf : options.getDirs())
  {
   File in = new File(outf);

   if( ! in.exists() )
   {
    System.err.println("Input directory '" + outf + "' doesn't exist");
    System.exit(1);
   }
   else if( ! in.isDirectory() )
   {
    System.err.println("'" + outf + "' is not a directory");
    System.exit(1);
   }
   
   indirs.add(in);
  }

  
  if( indirs.size() == 0 )
  {
   System.err.println("No files to process");
   return;
  }
  
  if( options.getOutDir() == null )
  {
   System.err.println("Output directory is not specified");
   return;
  }
  
  File outDir = new File( options.getOutDir() );

  if( outDir.isFile() )
  {
   System.err.println("Output path must point to a directory");
   return;
  }
  
  if( ! outDir.exists() && ! outDir.mkdirs() )
  {
   System.err.println("Can't create output directory");
   return;
  }
  
  int nThreads = -1;
  
  if( options.getThreadsNumber() != null )
  {
   try
   {
    nThreads = Integer.parseInt( options.getThreadsNumber() );
   }
   catch(Exception e)
   {
   }
   
   if( nThreads <= 0 || nThreads > 32 )
   {
    System.err.println("Invalid number of threads. Should be reasonable positive integer");
    return;
   }
  }
  else
   nThreads = 1;
  
  
  BlockingQueue<File> infiles = new LinkedBlockingQueue<File>();
  
  if( options.isPreloadFiles() || nThreads == 1 )
  {
   new CollectFilesTask(indirs, infiles, options).run();

   if(infiles.size() <= 1 )
   {
    System.err.println("No files to process");
    return;
   }
   
   System.out.println(String.valueOf(infiles.size()+" submissions in the queue"));
  }
  
  String sessionKey = null;
  DefaultHttpClient httpclient = null;
  
  PrintStream log = null;
  
  try
  {
   log = new PrintStream( new File(outDir,"log.txt") );
  }
  catch(FileNotFoundException e1)
  {
   System.err.println("Can't create log file: "+new File(outDir,"log.txt").getAbsolutePath());
   return;
  }

  if( options.getDatabaseURL() == null )
  {
   System.err.println("Database URI is required for remote operations");
   return;
  }
  else if(  ! options.getDatabaseURL().endsWith("/") )
   options.setDatabaseURI( options.getDatabaseURL()+"/" );
  
  if( options.getUser() == null )
  {
   System.err.println("User name is required for remote operations");
   return;
  }
  
  boolean ok = false;
  
  try
  {

   httpclient = new DefaultHttpClient();
   HttpPost httpost = new HttpPost(options.getDatabaseURL()+"Login");
   
   List<NameValuePair> nvps = new ArrayList<NameValuePair>();
   nvps.add(new BasicNameValuePair("username", options.getUser() ));
   nvps.add(new BasicNameValuePair("password", options.getPassword()!=null?options.getPassword():""));
   
   httpost.setEntity(new UrlEncodedFormEntity(nvps, HTTP.UTF_8));
   
   log.println("Trying to login onto the server");
   
   HttpResponse response = httpclient.execute(httpost);
   
   if( response.getStatusLine().getStatusCode() != HttpStatus.SC_OK )
   {
    log.println("Server response code is: "+response.getStatusLine().getStatusCode());
    return;
   }
   
   HttpEntity ent = response.getEntity();
   
   String respStr = EntityUtils.toString( ent ).trim();
   
   if( respStr.startsWith("OK:") )
   {
    System.out.println("Login successful");
    log.println("Login successful");
    sessionKey = respStr.substring(3);
   }
   else
   {
    log.println("Login failed: "+respStr);
    return;
   }
   
   EntityUtils.consume(ent);
   
   ok=true;
  }
  catch(Exception e)
  {
   log.println("ERROR: Login failed: "+e.getMessage());
   log.close();
   
   return;
  }
  finally
  {
   if( ! ok )
   {
    httpclient.getConnectionManager().shutdown();
    System.err.println("Login failed");
    
    System.exit(1);
   }
  }
  
  
  if(nThreads == 1)
  {
   Log psLog = new PrintStreamLog(log, false);

   new SubmitterTask("Main", options.getDatabaseURL() + "upload?" + Constants.sessionKey + "=" + sessionKey, infiles, outDir, options, psLog).run();

   psLog.shutdown();
  }
  else
  {
   Log psLog = new PrintStreamLog(log, true);

   psLog.write("Starting " + nThreads + " threads");

   ExecutorService exec = Executors.newFixedThreadPool(nThreads + 1);

   exec.execute(new CollectFilesTask(indirs, infiles, options));

   for(int i = 1; i <= nThreads; i++)
    exec.execute(new SubmitterTask("Thr" + i, options.getDatabaseURL() + "upload?" + Constants.sessionKey + "=" + sessionKey, infiles, outDir, options,
      psLog));

   try
   {
    exec.shutdown();

    exec.awaitTermination(72, TimeUnit.HOURS);
   }
   catch(InterruptedException e)
   {
   }

   psLog.shutdown();
  }

 }
 
 
 static class NullLog implements Log
 {
  @Override
  public void shutdown()
  {
  }

  @Override
  public void write(String msg)
  {
  }

  @Override
  public void printStackTrace(Exception e)
  {
  }
 }
 
 static class PrintStreamLog implements Log
 {
  private PrintStream log;
  private Lock        lock = new ReentrantLock();
  
  private boolean showThreads;

  PrintStreamLog(PrintStream l, boolean th)
  {
   log = l;
   showThreads = th;
  }

  public void shutdown()
  {
   log.close();
  }

  public void write(String msg)
  {
   lock.lock();

   try
   {
    if( showThreads )
     log.print("[" + Thread.currentThread().getName() + "] ");
    
    log.println(msg);
   }
   finally
   {
    lock.unlock();
   }
  }
  
  @Override
  public void printStackTrace(Exception e)
  {
   lock.lock();

   try
   {
    e.printStackTrace(log);
   }
   finally
   {
    lock.unlock();
   }
  }

 }

}
