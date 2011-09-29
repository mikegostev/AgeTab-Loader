package uk.ac.ebi.age.loader;

import java.util.List;

import org.kohsuke.args4j.Argument;
import org.kohsuke.args4j.Option;

public class Options
{
 @Option( name = "-r", usage="Process recursively")
 private boolean recursive;

 @Option( name = "-e", usage="Save server response")
 private boolean saveResponse;

 @Option( name = "-s", usage="Store files in database. Otherwise they will be verified but not stored")
 private boolean store;
 
 @Option( name = "-h", usage="Database base URL", metaVar="URL")
 private String databaseURL;
 
 @Option( name = "-u", usage="User name", metaVar="USER")
 private String user;

 @Option( name = "-p", usage="User password", metaVar="PASS")
 private String password;
 
 @Option( name = "-update", usage="Update submissions")
 private boolean update;
 
 @Option( name = "-new", usage="Load new submissions")
 private boolean newSub;

 @Option( name = "-i", usage="Use directory name as submission ID")
 private boolean dirAsID;

 @Option( name = "-m", usage="Use module file name as module ID")
 private boolean modFileAsID;

 @Option( name = "-o", usage="Output directory", metaVar="DIR")
 private String outDir;

 @Option( name = "-t", usage="Number of threads", metaVar="THRNUM")
 private String thrNum;

 @Option( name = "-c", usage="Continue loading")
 private boolean continueLd;

 @Option( name = "-f", usage="Refresh submissions")
 private boolean refresh;
 
 @Option( name = "-l", usage="Preload file list")
 private boolean preloadFiles;

 @Argument
 private List<String> dirs;

 public boolean isStore()
 {
  return store;
 }

 public String getDatabaseURL()
 {
  return databaseURL;
 }

 public String getUser()
 {
  return user;
 }

 public String getPassword()
 {
  return password;
 }

 public List<String> getDirs()
 {
  return dirs;
 }

 public void setDatabaseURI(String databaseURI)
 {
  this.databaseURL = databaseURI;
 }

 public boolean isSaveResponse()
 {
  return saveResponse;
 }

 public boolean isUpdateSubmissions()
 {
  return update;
 }

 public boolean isNewSubmissions()
 {
  return newSub;
 }

 public String getOutDir()
 {
  return outDir;
 }

 public boolean isRecursive()
 {
  return recursive;
 }

 public boolean isDirAsID()
 {
  return dirAsID;
 }

 public boolean isModFileAsID()
 {
  return modFileAsID;
 }

 public String getThreadsNumber()
 {
  return thrNum;
 }

 public boolean isContinue()
 {
  return continueLd;
 }
 
 public boolean isRefresh()
 {
  return refresh;
 }

 public boolean isPreloadFiles()
 {
  return preloadFiles;
 }

}
