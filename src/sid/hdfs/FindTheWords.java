/**
 * This is used to find the possible words (anagrams) based on given input word using Apache Spark Java API.
 * Sort the input characters into a key to search the anagram data list.
 * The data list is in HDFS.
 * This uses hadoop-0.23.3-dev-core.jar, spark-core_2.10-1.1.0.jar, scala-library-2.11.2.jar
 * Tested on hadoop-2.4.0, spark-1.0.0-bin-hadoop2.
 */
package sid.hdfs;

import java.util.Arrays;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;

/**
 * @author sidnan
 *
 */
public class FindTheWords {

	/**
	 * @param args
	 */
	@SuppressWarnings("serial")
	public static void main(String[] args) {
		final String KEY_VALUE_DELIMITER = "	";
		final String VALUE_DELIMITER = ";";

		String input = "cattier";
		
		/*
		 * Prepare Spark context
		 */
		SparkConf sparkConf = new SparkConf().setAppName("TestApp");
		JavaSparkContext ctx = new JavaSparkContext(sparkConf);
		JavaRDD<String> file = ctx.textFile("hdfs://localhost:9000/user/sidnan/input/MyDataFolder/AnagramDB/part-00000");

		/*
		 * Sort the input to prepare the key to search for the possible words
		 */
		char[] inputArray = input.toCharArray();
		Arrays.sort(inputArray);
		final String sortedInput = String.valueOf(inputArray);
		
		/*
		 * Search the data file for the possible words based on the key
		 */
		JavaRDD<String> possibleWords = file.filter(new Function<String, Boolean>() {
			public Boolean call(String s) { 
				return s.contains(sortedInput); 
			}
		});
		
		// the result can be cached in-memory for subsequent searches
		//possibleWords.cache();
		
		/*
		 * filter the key out and printout just the possbile words
		 */
		System.out.println("Possible words:-");
		for(String s:possibleWords.collect()) {
			if(s.contains(sortedInput) && s.contains(KEY_VALUE_DELIMITER)) {
				String[] line = s.split(KEY_VALUE_DELIMITER);
				if(line.length>0) {
					String word = line[1].replace(KEY_VALUE_DELIMITER, "");
					if(!word.isEmpty()) {
						for(String w: word.split(VALUE_DELIMITER)) {
							System.out.println(w);
						}
					}
				}
			}
		}
		System.out.println();
		
		ctx.stop();
	}

}
