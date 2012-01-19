#!/bin/sh

#Do not modify this file directly
#It is stored at git://github.com/EBIBioSamples/AgeTab-Loader.git/src/main/bin
#and installed by http://coconut.ebi.ac.uk:8085/browse/BSD-AGETABLOADER

base=${0%/*}/..;
current=`pwd`;

#If a java environment variable is not provided, then use a default
if [ -z $java ]
then
  java="/ebi/research/software/Linux_x86_64/opt/java/jdk1.7.0/bin/java"
fi

#args environment variable can be used to provide java arguments

#add proxy args
args="-Dhttp.proxyHost=wwwcache.ebi.ac.uk -Dhttp.proxyPort=3128 -Dhttp.nonProxyHosts=*.ebi.ac.uk -DproxyHost=wwwcache.ebi.ac.uk -DproxyPort=3128 -DproxySet=true $args"

#Combine jar files used into one variable
for file in `ls $base/lib`
do
  jars=$jars:$base/lib/$file;
done

#Make sure the classpath contains jar to run
#and other dependent jars
classpath="$jars:$base/config";

$java $args -classpath $classpath $@
