/**
 * This parse thru the list of words thru the HDFS files in a directory and find the anagrams
 * Using Apache Spark MapReduce Java API, prepare a file with <k, v> => <word sorted by characters, list of anagrams separated by semi-colon>
 * And save the result <k,v> in the HDFS file.
 * This uses hadoop-0.23.3-dev-core.jar, spark-core_2.10-1.1.0.jar, scala-library-2.11.2.jar
 * Tested on hadoop-2.4.0, spark-1.0.0-bin-hadoop2.
 */
package sid.hdfs;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

/**
 * @author sidnan
 *
 */
@SuppressWarnings("deprecation")
public class AnagramWordList {

	@SuppressWarnings({"serial" })
	public static void main(String[] args) throws Exception {
		
		SparkConf sparkConf = new SparkConf().setAppName("TestApp");
		JavaSparkContext ctx = new JavaSparkContext(sparkConf);
		
		// path/to/the/the/directory/that/holds/files => hdfs://<host>:<port>/user/<username>/directory
		// <file path, file content>
		JavaPairRDD<String, String> fileList = ctx.wholeTextFiles("hdfs://localhost:9000/user/sidnan/input/MyDataFolder/WordList");
		
		// read the content of the files
		Map<String, String> contentMap = fileList.collectAsMap();
		List<String> wordList = new ArrayList<String>();
		// the file content has list of words line by line
		for(String value: contentMap.values()) {
			// prepare collection list of words from the files
			wordList.addAll(Arrays.asList(value.split("\n")));
		}
		
		// convert the collection list to JavaRDD format for analysis
		JavaRDD<String> words = ctx.parallelize(wordList);
		
		/*
		 * mapper logic
		 * get the word -> sort characters to prepare the keys -> map the exact word to the created key
		 * example:-
		 * word: hello
		 * <key, value> => <ehllo, hello>
		 */
		JavaPairRDD<String, String> ones = words.mapToPair(new PairFunction<String, String, String>() {

			@Override
			public Tuple2<String, String> call(String s) throws Exception {
				char[] c = s.toCharArray();
				Arrays.sort(c);
				return new Tuple2<String, String>(String.valueOf(c), s);
			}
			
		});
		
		/*
		 * reducer logic
		 * group words by common key
		 * example:
		 * <key, value> => <iprst, sprit;stirp> 
		 */
		JavaPairRDD<String, String> counts = ones.reduceByKey(new Function2<String, String, String>() {
			
			@Override
			public String call(String s1, String s2) throws Exception {
				return s1 + ";" + s2;
			}
		});
		
		// path/to/the/the/directory/to/save/file => hdfs://<host>:<port>/user/<username>/directory/file
		counts.saveAsHadoopFile("hdfs://localhost:9000/user/sidnan/input/MyDataFolder/AnagramDB", Text.class, Text.class, TextOutputFormat.class);
		
		// To print & check the output on console
		/*
		List<Tuple2<String, String>> output = counts.collect();
		for (Tuple2<?,?> tuple : output) {
			System.out.println(tuple._1() + "==== " + tuple._2());
		}
		*/
				
		
		// close the created context		
		ctx.stop();
	}

}
