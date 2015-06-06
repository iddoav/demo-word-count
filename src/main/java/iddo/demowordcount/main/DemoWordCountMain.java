package iddo.demowordcount.main;

import iddo.demowordcount.service.spark.LocalSparkWordCounter;
import org.apache.log4j.BasicConfigurator;

/**
 * Created by iddo on 05/06/15.
 * A demo word counter program that makes use of a local spark application
 */
public class DemoWordCountMain {

    public static final String DEFAULT_INPUT_FILE = "target/classes/demo_input_file.txt";

    /**
     *
     * @param args the first argument should be some filepath on your system. The program will run a word count
     *             upon this file.
     */
    public static void main(String[] args){
        BasicConfigurator.configure();
        String inputFile;
        if (args.length==0) {
            inputFile = DEFAULT_INPUT_FILE;
        }else{
            inputFile = args[0];
        }
        LocalSparkWordCounter sparkDemoBatchProcessor = new LocalSparkWordCounter();
        sparkDemoBatchProcessor.countWords(inputFile);
    }

}
