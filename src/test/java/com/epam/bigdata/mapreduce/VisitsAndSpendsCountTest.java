package com.epam.bigdata.mapreduce;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by Ilya_Starushchanka on 9/4/2016.
 */
public class VisitsAndSpendsCountTest {

    private MapDriver<LongWritable, Text, Text, VisitsAndSpendsWritable> mapDriver;
    private ReduceDriver<Text, VisitsAndSpendsWritable, Text, VisitsAndSpendsWritable> reduceDriver;
    private MapReduceDriver<LongWritable, Text, Text, VisitsAndSpendsWritable, Text, VisitsAndSpendsWritable> mapReduceDriver;

    private String strIn1 = "1784dd0c5a02973f1407a5a57f022e12	20130606000104000	ZY5h13qZD4kIjU	mozilla/4.0 (compatible; msie 8.0; windows nt 5.1; trident/4.0; .net clr 2.0.50727; .net clr 3.0.4506.2152; .net clr 3.5.30729)	113.95.70.*	216	237	1	trqRTu1JX9q7aOMY5Spr-P	5370144f1fffebef4224c0c5925c528		mm_29939982_2608469_9578910	300	250	1	5	0	00fccc64a1ee2809348509b7ac2a97a5	227	3427	282163094301	0";
    private String strIn2 = "1784dd0c5a02973f1407a5a57f022e12	20130606000104000	ZY5h13qZD4kIjU	mozilla/4.0 (compatible; msie 8.0; windows nt 5.1; trident/4.0; .net clr 2.0.50727; .net clr 3.0.4506.2152; .net clr 3.5.30729)	113.95.71.*	216	237	1	trqRTu1JX9q7aOMY5Spr-P	5370144f1fffebef4224c0c5925c528		mm_29939982_2608469_9578910	300	250	1	5	0	00fccc64a1ee2809348509b7ac2a97a5	228	3427	282163094301	0";
    private String strIn3 = "1784dd0c5a02973f1407a5a57f022e12	20130606000104000	ZY5h13qZD4kIjU	mozilla/4.0 (compatible; msie 8.0; windows nt 5.1; trident/4.0; .net clr 2.0.50727; .net clr 3.0.4506.2152; .net clr 3.5.30729)	113.95.71.*	216	237	1	trqRTu1JX9q7aOMY5Spr-P	5370144f1fffebef4224c0c5925c528		mm_29939982_2608469_9578910	300	250	1	5	0	00fccc64a1ee2809348509b7ac2a97a5	228	3427	282163094301	0";


    private String ip1 = "113.95.70.*";
    private String ip2 = "113.95.71.*";

    @Before
    public void setup(){
        VisitsAndSpendsCount.CountMapper mapper = new VisitsAndSpendsCount.CountMapper();
        VisitsAndSpendsCount.CountReduce reduce = new VisitsAndSpendsCount.CountReduce();
        mapDriver = new MapDriver<>(mapper);
        reduceDriver = new ReduceDriver<>(reduce);
        mapReduceDriver = new MapReduceDriver<>(mapper, reduce);
    }

    @Test
    public void testMapper() throws IOException {
        mapDriver.withInput(new LongWritable(1), new Text(strIn1));
        mapDriver.withInput(new LongWritable(1), new Text(strIn2));
        mapDriver.withInput(new LongWritable(1), new Text(strIn3));
        mapDriver.withOutput(new Text(ip1), new VisitsAndSpendsWritable(1, 227));
        mapDriver.withOutput(new Text(ip2), new VisitsAndSpendsWritable(1, 228));
        mapDriver.withOutput(new Text(ip2), new VisitsAndSpendsWritable(1, 228));

        mapDriver.runTest();
    }

    @Test
    public void testReduce() throws IOException{
        List<VisitsAndSpendsWritable> values1 = new ArrayList<VisitsAndSpendsWritable>();
        values1.add(new VisitsAndSpendsWritable(1, 227));
        reduceDriver.withInput(new Text(ip1), values1);
        reduceDriver.withOutput(new Text(ip1), new VisitsAndSpendsWritable(1, 227));

        List<VisitsAndSpendsWritable> values2 = new ArrayList<VisitsAndSpendsWritable>();
        values2.add(new VisitsAndSpendsWritable(1, 228));
        values2.add(new VisitsAndSpendsWritable(1, 228));
        reduceDriver.withInput(new Text(ip2), values2);
        reduceDriver.withOutput(new Text(ip2), new VisitsAndSpendsWritable(2, 456));

        reduceDriver.runTest();
    }

    @Test
    public void testMapReduce() throws IOException{
        mapReduceDriver.withInput(new LongWritable(), new Text(strIn1));
        mapReduceDriver.withInput(new LongWritable(), new Text(strIn2));
        mapReduceDriver.withInput(new LongWritable(), new Text(strIn3));
        mapReduceDriver.withOutput(new Text(ip1), new VisitsAndSpendsWritable(1, 227));
        mapReduceDriver.withOutput(new Text(ip2), new VisitsAndSpendsWritable(2, 456));

        mapReduceDriver.runTest();
    }

}
