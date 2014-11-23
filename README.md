word-suggestor-spark-hadoop
===========================

Word suggestor using Anagram logic, Apache Spark, Hadoop.


# Used:-

* JDK 1.7
* hadoop-2.4.0
* spark-1.0.0-bin-hadoop2



* The `WordList` directory has the files with words Downloaded  from http://dreamsteep.com/projects/the-english-open-word-list.html. The word list helped.

* `lib` directory has the jars used in the classpath of this project.



# FSToHDFSApplication .java

This copies the files from the File System to HDFS using Hadoop Java API
This uses hadoop-0.23.3-dev-core.jar
Tested on hadoop-2.4.0.

**How to Run**
* Update the file with your hdfs path
* Prepare a jar
* ``` ./bin/hadoop jar /path/to/created/jar.jar sid.hdfs.FSToHDFSApplication ```.
*Example* ``` ./bin/hadoop jar /home/sidnan/workspace/test.jar sid.hdfs.FSToHDFSApplication ``` 


# AnagramWordList.java

This parse thru the list of words thru the HDFS files in a directory and find the anagrams.
Using Apache Spark MapReduce Java API, prepare a file with <k, v> => <word sorted by characters, list of anagrams separated by semi-colon>
And save the result <k,v> in the HDFS file.
This uses hadoop-0.23.3-dev-core.jar, spark-core_2.10-1.1.0.jar, scala-library-2.11.2.jar
Tested on hadoop-2.4.0, spark-1.0.0-bin-hadoop2.

**How to Run:-**
* Update the file with your hdfs path
* Prepare a jar 
* ``` ./bin/spark-submit --class sid.hdfs.AnagramWordList --master <master_name> /path/to/created/jar.jar ```
*Example* ``` ./bin/spark-submit --class sid.hdfs.AnagramWordList --master local /home/sidnan/workspace/test.jar ```



# FindTheWords.java

This is used to find the possible words (anagrams) based on given input word using Apache Spark Java API.
Sort the input characters into a key to search the anagram data list.
The data list is in HDFS.
This uses hadoop-0.23.3-dev-core.jar, spark-core_2.10-1.1.0.jar, scala-library-2.11.2.jar
Tested on hadoop-2.4.0, spark-1.0.0-bin-hadoop2.

**How to Run:-**
* Update the file with your hdfs path
* Prepare a jar 
* ```./bin/spark-submit --class sid.hdfs.FindTheWords --master <master_name> /path/to/creatd/jar.jar```
*Example* ```./bin/spark-submit --class sid.hdfs.FindTheWords --master local /home/sidnan/workspace/test.jar```

