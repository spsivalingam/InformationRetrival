This is a README file 
-- parameters---
INPUT_DIR =assignment
CLASS_COMPILE = DocWordCount.java, TermFrequency.java, TFIDF.java, Search.java
CLASS_JAR = DocWordCount.jar, TermFrequency.jar, TFIDF.jar, Search.jar
CLASS_NAME = DocWordCount, TermFrequency, TFIDF, Search
-- Running the program --

* Before you run the sample, you must create input and output locations in HDFS. Use the following commands to create the input directory/user/cloudera/INPUT_DIR/input in HDFS:
$ sudo su hdfs
$ hadoop fs -mkdir /user/cloudera
$ hadoop fs -chown cloudera /user/cloudera
$ exit
$ sudo su cloudera
$ hadoop fs -mkdir /user/cloudera/assignment /user/cloudera/assignment/input 

* Move the text files of the canterbury corpus provided to use as input, and move them to the/user/cloudera/assignment/input directory in HDFS. 

$ hadoop fs -put * /user/cloudera/assignment/input 

* Compile the  class.
To compile in a package installation of CDH:

$ mkdir -p build
$ javac -cp /usr/lib/hadoop/*:/usr/lib/hadoop-mapreduce/* CLASS_COMPILE -d build -Xlint 

* Create a JAR file for the application.
$ jar -cvf CLASS_JAR -C build/ . 

* Run the application for DocWordCount, TermFrequency from the JAR file, passing the paths to the input and output directories in HDFS.
$ hadoop jar CLASS_JAR org.myorg.CLASS_NAME /user/cloudera/assignment/input /user/cloudera/CLASS_NAME/output


* Run the application for TFIDF, from the JAR file, passing the paths to the input and output directories in HDFS.
$ hadoop jar CLASS_JAR org.myorg.CLASS_JAR /user/cloudera/assignment/input /user/cloudera/output /user/cloudera/CLASS_NAME/output


* Run the application for Search, from the JAR file, passing the paths to the input and output directories in HDFS.
$ hadoop jar CLASS_JAR org.myorg.CLASS_JAR /user/cloudera/TFIDF/output /user/cloudera/output /user/cloudera/CLASS_NAME/output


* Output can be seen using the below command:
$ hadoop fs -cat /user/cloudera/CLASS_NAME/output/*

* If you want to run the sample again, you first need to remove the output directory. Use the following command.
$ hadoop fs -rm -r /user/cloudera/CLASS_NAME/output 

-- CONTACT --

* Sivalingam Subbiah (ssubbiah@uncc.edu)
