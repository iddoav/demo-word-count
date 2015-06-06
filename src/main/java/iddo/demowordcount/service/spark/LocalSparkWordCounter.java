package iddo.demowordcount.service.spark;

import iddo.demowordcount.service.WordCounter;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

/**
 * Created by iddo on 05/06/15.
 * A simple word counter implementation that is implemented with a local spark context
 */
public class LocalSparkWordCounter implements WordCounter {

    static Logger logger = Logger.getLogger(WordCounter.class);


    public void countWords(String inputFilePath) {

        logger.info("Starting the computation");
        JavaSparkContext sc = getJavaSparkContext();

        JavaRDD<String> logData = sc.textFile(inputFilePath).cache();
        JavaPairRDD<String, Long> pairs = logData.flatMapToPair(new WordCountMapper());
        JavaPairRDD<String, Long> counts = pairs.reduceByKey(new WordCountReducer());
        counts.collectAsMap().forEach((s,l)->System.out.println(s + " : " + l));
        sc.close();
        logger.info("The computation has ended");
    }

    private JavaSparkContext getJavaSparkContext() {
        SparkConf conf = new SparkConf().setAppName("Demo Spark batch processor")
                .setMaster("local");
        return new JavaSparkContext(conf);
    }
}
