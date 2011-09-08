package uk.ac.ebi.age.loader;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.List;

import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.HttpStatus;
import org.apache.http.NameValuePair;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.mime.MultipartEntity;
import org.apache.http.entity.mime.content.FileBody;
import org.apache.http.entity.mime.content.StringBody;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.message.BasicNameValuePair;
import org.apache.http.protocol.HTTP;
import org.apache.http.util.EntityUtils;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;

import uk.ac.ebi.age.admin.shared.Constants;
import uk.ac.ebi.age.admin.shared.SubmissionConstants;
import uk.ac.ebi.age.ext.submission.Status;


public class ATLoader
{
 static final String usage = "java -jar ATLoader.jar [options...] <input dir> [ ... <input dir> ]";

 /**
  * @param args
  */
 public static void main(String[] args)
 {
  Options options = new Options();
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
  
  
  List<File> infiles = new ArrayList<File>();
  
 
  for( String outf : options.getDirs() )
  {
   File in = new File( outf );
   
   
   if( in.isDirectory() )
   {
    for( File sdir : in.listFiles() )
     if( sdir.isDirectory() )
      infiles.add(sdir);
   }
   else if( in.exists() )
   {
    System.err.println("ERROR: '"+in.getAbsolutePath()+"' is not a directory");
    System.exit(2);
   }
   else
   {
    System.err.println("Input directory '"+in.getAbsolutePath()+"' doesn't exist");
    return;
   }
  }
  
  
  if( infiles.size() == 0 )
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
  
  String sessionKey = null;
  DefaultHttpClient httpclient = null;
  
  PrintWriter log = null;
  
  try
  {
   log = new PrintWriter( new File(outDir,"log.txt") );
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
  
  
  
  List<File> modules = new ArrayList<File>();
  List<File> attachments = new ArrayList<File>();
   
  try
  {
   for(File sbmDir : infiles)
   {

    HttpPost post = new HttpPost( options.getDatabaseURL()+"upload?"+Constants.sessionKey+"="+sessionKey );

    String key = String.valueOf(System.currentTimeMillis());
    
     MultipartEntity reqEntity = new MultipartEntity();

     String sbmId = sbmDir.getName();
     
     if( sbmId.startsWith(".") )
      sbmId = null;

     modules.clear();
     attachments.clear();
     
     for( File f : sbmDir.listFiles() )
     {
      if( ! f.isFile() || f.getName().startsWith(".") )
       continue;
      
      if( f.getName().endsWith(".age.txt") )
       modules.add(f);
      else
       attachments.add(f);
     }
     
     try
     {

      Status sts;

      if( options.isNewSubmissions() )
       sts = Status.NEW;
      else if( options.isUpdateSubmissions() )
       sts = Status.UPDATE;
      else
       sts = Status.UPDATEORNEW;

      reqEntity.addPart(Constants.uploadHandlerParameter, new StringBody(SubmissionConstants.SUBMISSON_COMMAND));
      reqEntity.addPart(SubmissionConstants.SUBMISSON_KEY, new StringBody(key));
      reqEntity.addPart(SubmissionConstants.SUBMISSON_STATUS, new StringBody(sts.name()) );

      if(!options.isStore())
       reqEntity.addPart(SubmissionConstants.VERIFY_ONLY, new StringBody("on"));

      if( sbmId != null )
       reqEntity.addPart(SubmissionConstants.SUBMISSON_ID, new StringBody(sbmId));
      
      File descr = new File(sbmDir,".description");

      if( descr.canRead() )
       reqEntity.addPart(SubmissionConstants.SUBMISSON_DESCR, new FileBody(descr, "text/plain","UTF-8"));

      int n=0;
      
      for( File modFile : modules )
      {
       n++;
       
       String modName = modFile.getName().substring(0,modFile.getName().length()-8);

       reqEntity.addPart(SubmissionConstants.MODULE_ID+n, new StringBody(modName) );
       
       File modDesc = new File( sbmDir, ".description."+modName);
       
       if( modDesc.canRead() )
        reqEntity.addPart(SubmissionConstants.MODULE_DESCRIPTION + n, new FileBody(modDesc, "text/plain", "UTF-8"));
       else
        reqEntity.addPart(SubmissionConstants.MODULE_DESCRIPTION + n, new StringBody(modName) );

       reqEntity.addPart(SubmissionConstants.MODULE_STATUS + n, new StringBody(sts.name()));

       reqEntity.addPart(SubmissionConstants.MODULE_FILE + n, new FileBody(modFile, "text/plain", "UTF-8"));
      }
      

      for( File attFile : attachments )
      {
       n++;
       
       reqEntity.addPart(SubmissionConstants.ATTACHMENT_ID+n, new StringBody(attFile.getName()) );
       
       File attDesc = new File( sbmDir, ".description."+attFile.getName());
       
       if( attDesc.canRead() )
        reqEntity.addPart(SubmissionConstants.ATTACHMENT_DESC + n, new FileBody(attDesc, "text/plain","UTF-8"));
       else
        reqEntity.addPart(SubmissionConstants.ATTACHMENT_DESC + n, new StringBody(attFile.getName()) );

       reqEntity.addPart(SubmissionConstants.ATTACHMENT_STATUS + n, new StringBody(sts.name()));

       reqEntity.addPart(SubmissionConstants.ATTACHMENT_FILE + n, new FileBody(attFile, "application/binary"));

       File attGlobal = new File( sbmDir, ".global."+attFile.getName());
       
       if( attGlobal.exists() )
        reqEntity.addPart(SubmissionConstants.ATTACHMENT_GLOBAL + n, new StringBody("on"));
      }

     }
     catch(UnsupportedEncodingException e)
     {
      log.println("ERROR: UnsupportedEncodingException: " + e.getMessage());
      return;
     }

     post.setEntity(reqEntity);

     HttpResponse response;
     try
     {
      response = httpclient.execute(post);

      if(response.getStatusLine().getStatusCode() != HttpStatus.SC_OK)
      {
       log.println("Server response code is: " + response.getStatusLine().getStatusCode());
       return;
      }

      HttpEntity ent = response.getEntity();

      String respStr = EntityUtils.toString(ent);

      int pos = respStr.indexOf("OK-" + key);

      if(pos == -1)
      {
       log.println("ERROR: Invalid server response : " + respStr);
       continue;
      }

      pos = pos + key.length() + 5;
      String respStat = respStr.substring(pos, respStr.indexOf(']', pos));

      log.println("Submission status: " + respStat);
      System.out.println("Submission status: " + respStat);

      if(options.isSaveResponse())
      {
       log.println("Writing response");
       File rspf = new File(outDir, sbmDir.getName() + '.' + respStat);

       PrintWriter pw = new PrintWriter(rspf, "UTF-8");

       pw.write(respStr);
       pw.close();
      }

      EntityUtils.consume(ent);
     }
     catch(Exception e)
     {
      log.println("ERROR: IO error: " + e.getMessage());
      return;
     }

    log.println("File '"+sbmDir.getName()+ "' done");
    System.out.println("File '"+sbmDir.getName()+ "' done");

   }
  }
  finally
  {
   if(httpclient != null)
    httpclient.getConnectionManager().shutdown();

   log.close();
  }
 }

}
