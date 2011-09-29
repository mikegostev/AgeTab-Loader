package uk.ac.ebi.age.loader;

import java.io.File;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.BlockingQueue;

public class CollectFilesTask implements Runnable
{
 private Collection<File> rootDirs;
 private Set<String> processedDirs;
 private BlockingQueue<File> infiles;
 private Options options;
 
 public CollectFilesTask( Collection<File> rds, BlockingQueue<File> inf, Options opt )
 {
  rootDirs = rds;
  infiles = inf;
  options = opt;
 }
 
 
// public static void collect(Set<File> indirs, BlockingQueue<File> infiles2, Options options2)
// {
//  for(File d : indirs)
//   new CollectFilesTask(d, infiles2, options2).collectInput(d);
//
//  while(true)
//  {
//   try
//   {
//    infiles2.put(new File(""));
//    return;
//   }
//   catch(InterruptedException e)
//   {
//   }
//  }
// }
 
 @Override
 public void run()
 {
  processedDirs = new HashSet<String>();
  
  for( File rd : rootDirs )
   collectInput(rd);
  
  while(true)
  {
   try
   {
    infiles.put( new File("") );
    
    processedDirs = null;

    return;
   }
   catch(InterruptedException e)
   {
   }
  }
  
 }
 
 private void collectInput( File in )
 {
  File[] files = in.listFiles();
  
  boolean added = false;
  
  dir: for( File f : files )
  {
   if(f.isDirectory() && !processedDirs.contains(f.getAbsolutePath()) && options.isRecursive())
   {
    collectInput(f);
    processedDirs.add(f.getAbsolutePath());
   }
   else if(!added && f.getName().endsWith(".age.txt") && f.isFile())
   {
    while(true)
    {
     try
     {
      infiles.put(in);
      
      added = true;
      
      continue dir;
     }
     catch(InterruptedException e)
     {
     }
    }
   }
  }
 }



}
