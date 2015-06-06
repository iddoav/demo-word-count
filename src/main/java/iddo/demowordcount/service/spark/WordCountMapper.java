package iddo.demowordcount.service.spark;

import org.apache.spark.api.java.function.PairFlatMapFunction;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

/**
 *  Created by iddo on 05/06/15.
 * A mapper that for a given text-line emit pairs of single word counts for each of the words in that line
 * Note that:
 * a. This code is easy to test
 * b. The internal implementation may contain blocks which are not strictly functional
 */
public class WordCountMapper implements PairFlatMapFunction<String, String, Long> {

    @Override
    public Iterable<Tuple2<String, Long>> call(String s) throws Exception {
        List<String> words = Arrays.asList((s.split(" ")));
        return words.stream().map(word -> new Tuple2<>(word, 1l))
                .collect(Collectors.toList());
    }
}
