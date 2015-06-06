package iddo.demowordcount.service.spark;

import org.junit.Assert;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import scala.Tuple2;

import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 * Created by iddo on 05/06/15.
 * Unit test suite for the WordCountMapper class
 */
public class WordCountMapperTest {

    private String line1;
    private WordCountMapper wordCountMapper;
    private  Iterable<Tuple2<String, Long>> lineWordPairs;

    @Before
    public void setUp() throws Exception {
        line1 = "The barber shop of the cool barber is closed.";
        wordCountMapper = new WordCountMapper();
        lineWordPairs = wordCountMapper.call(line1);
    }

    @After
    public void tearDown() throws Exception {

    }

    @Test
    public void testCallShopCounts() throws Exception {
        Assert.assertEquals(1, getStream(lineWordPairs).filter(tuple1 -> tuple1._1().equals("shop")).count());
    }

    @Test
    public void testCallBarberCounts() throws Exception {
        Assert.assertEquals(2, getStream(lineWordPairs).filter(tuple1 -> tuple1._1().equals("barber")).count());
    }

    @Test
    public void testCallTupleStructure() throws Exception {
        Assert.assertTrue(getStream(lineWordPairs).allMatch(tuple1 -> tuple1._2().equals(1l)));
    }

    private Stream<Tuple2<String, Long>> getStream(Iterable<Tuple2<String, Long>> lineWordPairs) {
        return StreamSupport.stream(lineWordPairs.spliterator(), false);
    }

}