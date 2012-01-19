package uk.ac.ebi.age.loader;

public interface Log
{
 void shutdown();
 void write(String msg);
 void printStackTrace(Exception e);
}
