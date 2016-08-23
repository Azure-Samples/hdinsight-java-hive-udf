package com.microsoft.example;

import junit.framework.Assert;

import org.apache.hadoop.hive.ql.udf.ptf.MatchPath;
import org.apache.hadoop.io.Text;
import org.junit.Test;

public class timestampconvTest {

    @Test
    public void testUDF1() {
        timestampconv example = new timestampconv();
        String [] text_array= new String[2];
        text_array[0] = new String("1996-03-29T12:40:30");
        text_array[1] = new String("yyyy-mm-ddthh:mm:ss[.mmm]");
        Assert.assertEquals("1996-03-29 12:40:30", example.evaluate(text_array).toString());
    }

    @Test
    public void testUDF2() {
        timestampconv example = new timestampconv();
        String [] text_array= new String[2];
        text_array[0] = new String("19960229 12:40:30.123");
        text_array[1] = new String("YYYYMMDD[ hh:mm:ss[.mmm]]");
        Assert.assertEquals("1996-02-29 12:40:30.123", example.evaluate(text_array).toString());
    }

    @Test
    public void testUDF3() {
        timestampconv example = new timestampconv();
        String [] text_array= new String[2];
        text_array[0] = new String("19960129");
        text_array[1] = new String("YYYYMMDD[ hh:mm:ss[.mmm]]");
        Assert.assertEquals("1996-01-29 00:00:00", example.evaluate(text_array).toString());
    }

    @Test
    public void testUDF4() {
        timestampconv example = new timestampconv();
        String [] text_array= new String[2];
        text_array[0] = new String("1996-05-29T12:40:30.2384");
        text_array[1] = new String("YYYY-MM-DDTHH:MM:SS[.MMM]");
        Assert.assertEquals("1996-05-29 12:40:30.2384", example.evaluate(text_array).toString());
    }

    @Test
    public void testUDF5() {
        timestampconv example = new timestampconv();
        String [] text_array= new String[2];
        text_array[0] = new String("96/02/21");
        text_array[1] = new String("Y/M/D");
        Assert.assertEquals("1996-02-21 00:00:00", example.evaluate(text_array).toString());
    }

    @Test
    public void testUDF6() {
        timestampconv example = new timestampconv();
        String [] text_array= new String[2];
        text_array[0] = new String("12/15/2010");
        text_array[1] = new String("M/D/Y");
        Assert.assertEquals("2010-12-15 00:00:00", example.evaluate(text_array).toString());
    }

    //Negative Test
    @Test
    public void testUDF7() {
        timestampconv example = new timestampconv();
        String [] text_array= new String[2];
        text_array[0] = new String("12:40:30.132");
        text_array[1] = new String("YYYY-MM-DDTHH:MM:SS[.MMM]");
        System.out.println(example.evaluate(text_array));
        //Assert.assertEquals("null", example.evaluate(text_array));
    }

    @Test
    public void testUDFNullCheck() {
        timestampconv example = new timestampconv();
        Assert.assertNull(example.evaluate(null));
    }
}