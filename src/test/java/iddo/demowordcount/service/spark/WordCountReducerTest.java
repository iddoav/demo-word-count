package iddo.demowordcount.service.spark;

import org.junit.Before;
import org.junit.Test;
import org.junit.Assert;

/**
 * Created by iddo on 05/06/15. WordCountReducer
 * Unit test suite for the WordCountReducer class
 */
public class WordCountReducerTest {

    private WordCountReducer wordCountReducer;

    @Before
    public void setUp() throws Exception {
        wordCountReducer = new WordCountReducer();
    }

    @Test
    public void testCall() throws Exception {
        Assert.assertEquals(8l, wordCountReducer.call(3l, 5l).longValue());
    }
}