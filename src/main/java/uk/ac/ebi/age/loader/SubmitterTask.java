package uk.ac.ebi.age.loader;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;

import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.HttpStatus;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.mime.MultipartEntity;
import org.apache.http.entity.mime.content.FileBody;
import org.apache.http.entity.mime.content.StringBody;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.util.EntityUtils;

import uk.ac.ebi.age.admin.shared.Constants;
import uk.ac.ebi.age.admin.shared.SubmissionConstants;
import uk.ac.ebi.age.ext.submission.Status;

import com.pri.util.stream.StreamPump;

public class SubmitterTask implements Callable<Integer>
{

 public static final int     ST_OK           = 0;
 public static final int     ST_INV_ENC      = 1;
 public static final int     ST_RMT_IO_ERR   = 2;
 public static final int     ST_INV_SRV_RESP = 3;
 public static final int     ST_EXPT         = 4;
 public static final int     ST_VLD_ERR      = 5;
 public static final int     ST_LCL_IO_ERR   = 6;
 public static final int     ST_NO_ID        = 7;
 
 private Log                 log;
 private String              threadName;
 private File                outDir;
 private BlockingQueue<File> infiles;
 private Options             options;
 private String              url;

 public int status = ST_OK;
 
 public SubmitterTask(String threadName, String url, BlockingQueue<File> fqueue, File oDir, Options opt, Log log)
 {
  this.threadName = threadName;
  this.log = log;

  outDir = oDir;
  infiles = fqueue;

  this.options = opt;
  this.url = url;
 }

 @Override
 public Integer call()
 {
  Thread.currentThread().setName(threadName);

  HttpClient httpclient = new DefaultHttpClient();

  List<File> modules = new ArrayList<File>();
  List<File> attachments = new ArrayList<File>();

  int counter = 1;

  try
  {
   submissions: while(true)
   {
    File sbmDir = null;

    try
    {
     sbmDir = infiles.take();
    }
    catch(InterruptedException e1)
    {
     continue;
    }

    if(sbmDir.getName().length() == 0)
    {
     while(true)
     {
      try
      {
       infiles.put(sbmDir);
       return status;
      }
      catch(InterruptedException e)
      {
      }
     }

    }

    long stTime = System.currentTimeMillis();

    log.write("Processing: " + sbmDir.getAbsolutePath());

    HttpPost post = new HttpPost(url);

    String key = String.valueOf(System.currentTimeMillis());

    MultipartEntity reqEntity = new MultipartEntity();

    String sbmId = null;

    modules.clear();
    attachments.clear();

    for(File f : sbmDir.listFiles())
    {
     if(!f.isFile() || f.getName().startsWith("."))
      continue;

     if(f.getName().endsWith(".age.txt"))
      modules.add(f);
     else
      attachments.add(f);
    }

    try
    {

     Status sts;

     if(options.isNewSubmissions())
      sts = Status.NEW;
     else if(options.isUpdateSubmissions())
      sts = Status.UPDATE;
     else
      sts = Status.UPDATEORNEW;

     reqEntity.addPart(Constants.uploadHandlerParameter, new StringBody(Constants.SUBMISSON_COMMAND));
     reqEntity.addPart(SubmissionConstants.SUBMISSON_KEY, new StringBody(key));
     reqEntity.addPart(SubmissionConstants.SUBMISSON_STATUS, new StringBody(sts.name()));

     if(options.getTagString() != null)
      reqEntity.addPart(SubmissionConstants.SUBMISSON_TAGS, new StringBody(options.getTagString()));

     if(!options.isStore())
      reqEntity.addPart(SubmissionConstants.VERIFY_ONLY, new StringBody("on"));

     File sbId = new File(sbmDir, ".id");

     if(sbId.canRead())
     {
      try
      {
       sbmId = readFile(sbId).trim();
      }
      catch(IOException e)
      {
       log.write("ERROR: Can't read file: " + sbId.getAbsolutePath());
       status = ST_LCL_IO_ERR;
       continue;
      }
     }

     if(sbmId == null && options.isDirAsID())
      sbmId = sbmDir.getName();
     else if(sts != Status.NEW && (sbmId == null || sbmId.length() == 0))
     {
      log.write("ERROR: Submission ID is not specified");
      status = ST_NO_ID;
      continue;
     }

     if(sbmId != null && sbmId.length() != 0)
     {
      if(options.isContinue())
      {
       File sresp = new File(outDir, sbmId + ".SUCCESS");

       boolean uptodate = false;

       if(sresp.exists())
       {
        uptodate = true;

        if(options.isRefresh())
        {
         for(File f : sbmDir.listFiles())
         {
          if(f.lastModified() > sresp.lastModified())
          {
           uptodate = false;
           break;
          }
         }
        }
       }

       if(uptodate)
       {
        log.write("Submission '" + sbmId + "' is up-to-date");
        continue submissions;
       }
      }

      reqEntity.addPart(SubmissionConstants.SUBMISSON_ID, new StringBody(sbmId));
     }

     if(sbmId == null || sbmId.length() == 0)
      sbmId = "_$" + (counter++);

     File descr = new File(sbmDir, ".description");

     if(descr.canRead())
     {
      try
      {
       reqEntity.addPart(SubmissionConstants.SUBMISSON_DESCR, new StringBody(readFile(descr)));
      }
      catch(IOException e)
      {
       log.write("ERROR: Can't read file: " + descr.getAbsolutePath());
       status = ST_LCL_IO_ERR;
       continue;
      }
     }

     if(options.getTagString() == null)
     {
      File tagFile = new File(sbmDir, ".tags");

      if(tagFile.canRead())
      {
       try
       {
        reqEntity.addPart(SubmissionConstants.SUBMISSON_TAGS, new StringBody(readFile(tagFile)));
       }
       catch(IOException e)
       {
        log.write("ERROR: Can't read file: " + tagFile.getAbsolutePath());
        status = ST_LCL_IO_ERR;
        continue;
       }
      }
     }

     int n = 0;

     for(File modFile : modules)
     {
      n++;

      log.write("Processing module: " + modFile.getAbsolutePath());

      String modId = null;

      File modIdFile = new File(sbmDir, ".id." + modFile.getName());

      if(modIdFile.canRead())
      {
       try
       {
        modId = readFile(modIdFile).trim();
       }
       catch(IOException e)
       {
        log.write("ERROR: Can't read file: " + modIdFile.getAbsolutePath());
        status = ST_LCL_IO_ERR;
        continue submissions;
       }
      }

      if(modId != null && modId.length() == 0)
       modId = null;

      if(modId == null)
      {
       if(options.isModFileAsID())
        modId = modFile.getName().substring(0, modFile.getName().length() - 8);
       else if(sts != Status.NEW)
       {
        log.write("ERROR: Module ID is not specified");
        status = ST_NO_ID;
        continue submissions;
       }
      }

      reqEntity.addPart(SubmissionConstants.MODULE_ID + n, new StringBody(modId));

      File modDescFile = new File(sbmDir, ".description." + modFile.getName());

      String modDesc = null;

      if(modDescFile.canRead())
      {
       try
       {
        modDesc = readFile(modDescFile);
       }
       catch(IOException e)
       {
        log.write("ERROR: Can't read file: " + modDescFile.getAbsolutePath());
        status = ST_LCL_IO_ERR;
        continue submissions;
       }
      }

      if(modDesc == null)
       modDesc = modFile.getName();

      File modTagsFile = new File(sbmDir, ".tags." + modFile.getName());

      if(modTagsFile.canRead())
      {
       try
       {
        reqEntity.addPart(SubmissionConstants.MODULE_TAGS + n, new StringBody(readFile(modTagsFile)));
       }
       catch(IOException e)
       {
        log.write("ERROR: Can't read file: " + modTagsFile.getAbsolutePath());
        continue submissions;
       }
      }

      reqEntity.addPart(SubmissionConstants.MODULE_DESCRIPTION + n, new StringBody(modDesc));

      reqEntity.addPart(SubmissionConstants.MODULE_STATUS + n, new StringBody(sts.name()));

      reqEntity.addPart(SubmissionConstants.MODULE_FILE + n, new FileBody(modFile, "text/plain", "UTF-8"));
     }

     for(File attFile : attachments)
     {
      n++;

      String attId = null;

      File attIdFile = new File(sbmDir, ".id." + attFile.getName());

      if(attIdFile.canRead())
      {
       try
       {
        attId = readFile(attIdFile).trim();
       }
       catch(IOException e)
       {
        log.write("ERROR: Can't read file: " + attIdFile.getAbsolutePath());
        status = ST_LCL_IO_ERR;
        continue submissions;
       }
      }

      if(attId != null && attId.length() == 0)
       attId = null;

      if(attId == null)
       attId = attFile.getName();

      reqEntity.addPart(SubmissionConstants.ATTACHMENT_ID + n, new StringBody(attId));

      File attDescFile = new File(sbmDir, ".description." + attFile.getName());

      String attDesc = null;

      if(attDescFile.canRead())
      {
       try
       {
        attDesc = readFile(attDescFile);
       }
       catch(IOException e)
       {
        log.write("ERROR: Can't read file: " + attDescFile.getAbsolutePath());
        status = ST_LCL_IO_ERR;
        continue submissions;
       }
      }

      if(attDesc == null)
       attDesc = attFile.getName();

      File attTagsFile = new File(sbmDir, ".tags." + attFile.getName());

      if(attTagsFile.canRead())
      {
       try
       {
        reqEntity.addPart(SubmissionConstants.ATTACHMENT_TAGS + n, new StringBody(readFile(attTagsFile)));
       }
       catch(IOException e)
       {
        log.write("ERROR: Can't read file: " + attTagsFile.getAbsolutePath());
        status = ST_LCL_IO_ERR;
        continue submissions;
       }
      }

      reqEntity.addPart(SubmissionConstants.ATTACHMENT_DESC + n, new StringBody(attDesc));

      reqEntity.addPart(SubmissionConstants.ATTACHMENT_STATUS + n, new StringBody(sts.name()));

      reqEntity.addPart(SubmissionConstants.ATTACHMENT_FILE + n, new FileBody(attFile, "application/binary"));

      File attGlobal = new File(sbmDir, ".global." + attFile.getName());

      if(attGlobal.exists())
       reqEntity.addPart(SubmissionConstants.ATTACHMENT_GLOBAL + n, new StringBody("on"));
     }

    }
    catch(UnsupportedEncodingException e)
    {
     log.write("ERROR: UnsupportedEncodingException: " + e.getMessage());
     return ST_INV_ENC;
    }

    post.setEntity(reqEntity);

    HttpResponse response;
    try
    {
     response = httpclient.execute(post);

     if(response.getStatusLine().getStatusCode() != HttpStatus.SC_OK)
     {
      log.write("Server response code is: " + response.getStatusLine().getStatusCode());
      return ST_INV_SRV_RESP;
     }

     HttpEntity ent = response.getEntity();

     String respStr = EntityUtils.toString(ent);

     int pos = respStr.indexOf("OK-" + key);

     if(pos == -1)
     {
      log.write("ERROR: Invalid server response : " + respStr);
      status = ST_INV_SRV_RESP;
      continue;
     }

     pos = pos + key.length() + 5;
     String respStat = respStr.substring(pos, respStr.indexOf(']', pos));

     log.write("Submission status: " + respStat);
     System.out.println("Submission status: " + respStat);

     if( respStat.equals("ERROR") )
      status = ST_VLD_ERR;
     
     if(options.isSaveResponse())
     {
      log.write("Writing response");
      File rspf = new File(outDir, sbmId + '.' + respStat);

      PrintWriter pw = new PrintWriter(rspf, "UTF-8");

      pw.write(respStr);
      pw.close();
     }

     EntityUtils.consume(ent);
    }
    catch(Exception e)
    {
     log.write("ERROR: IO error: " + e.getMessage());
     return ST_RMT_IO_ERR;
    }

    long endTime = System.currentTimeMillis() - stTime;

    log.write("Submission '" + sbmDir.getAbsolutePath() + "' done (" + endTime + "ms)");
    System.out.println("File '" + sbmDir.getAbsolutePath() + "' done (" + endTime + "ms) " + infiles.size() + " in the queue");

   }
  }
  catch(Exception e)
  {
   log.write("Exception in the thread: " + e.getMessage());
   log.printStackTrace(e);
   
   return ST_EXPT;
  }
  finally
  {
   if(httpclient != null)
    httpclient.getConnectionManager().shutdown();
  }
 }

 static String readFile(File f) throws IOException
 {
  ByteArrayOutputStream baos = new ByteArrayOutputStream();

  FileInputStream fis = new FileInputStream(f);

  StreamPump.doPump(fis, baos, true);

  return new String(baos.toByteArray(), "UTF-8");
 }
}
