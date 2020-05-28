Authors: Daniel Chavez and Brandon Jungen

Scala-
First untar the tar file and it will place everything into the directory that you specified. The scala file is under
currentDirectory/src/main/scala/TermProjectAnalysis.scala
Use sbt package to package that scala file into a jar. Once it is done there will be a jar under
currentDirectory/target/scala-2.11/termproject_2.11-1.0.jar
Spark and HDFS will need to be running in order for this file to run
Then you will need spark running in order to run the job on a spark cluster. The command is
$SPARK_HOME/bin/spark-submit --master spark://masternode:port --deploy-mode cluster --class TermProjectAnalysis --supervise target/scala-2.11/termproject_2.11-1.0.jar
This will then run the spark program and will write outputs madison:65000 for the HDFS since that was the HDFS namenode and port we used.
To run on your own you would need to change the namenode and port for reading and writing.

JupyterNotebook(Python 3)
Run the above code and download the resulting csv files locally.
Make sure you have the csv files in the same directory as the notebook.
Start the notebook and click run all cells.
I have divided the cells into sections for each csv and we will elaborate on how we interpreted the data in our report.
