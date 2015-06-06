package iddo.demowordcount.service.spark;

/**
 * Created by iddo on 05/06/15.
 * A reducer that contains a sum function logic that is used for reducing in an associative fashion word counts.
 * * Note that:
 * a. This code is easy to test
 * b. The internal implementation may contain blocks which are not strictly functional
 */
public class WordCountReducer implements org.apache.spark.api.java.function.Function2<Long, Long, Long> {
    @Override
    public Long call(Long aLong, Long aLong2) throws Exception {
        return aLong + aLong2;
    }
}
