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
public class TagsCountTest {

   private MapDriver<LongWritable, Text, Text, IntWritable> mapDriver;
   private ReduceDriver<Text, IntWritable, Text, IntWritable> reduceDriver;
   private MapReduceDriver <LongWritable, Text, Text, IntWritable,Text, IntWritable> mapReduceDriver;

   private String strIn1 = "282163000648 motorcycle,atv ON CPC BROAD http://www.miniinthebox.com/motorcycle-atv_c9822";
   private String strIn2 = "282163000649 motorcycle ON CPC BROAD http://www.miniinthebox.com/motorcycle-atv_c9822";

   private String wordOut1 = "MOTORCYCLE";
   private String wordOut2 = "ATV";

   @Before
   public void setup(){
      TagsCount.WordCountMapper mapper = new TagsCount.WordCountMapper();
      TagsCount.WordCountReduce reduce = new TagsCount.WordCountReduce();
      mapDriver = new MapDriver<>(mapper);
      reduceDriver = new ReduceDriver<>(reduce);
      mapReduceDriver = new MapReduceDriver<>(mapper, reduce);
   }

   @Test
   public void testMapper() throws IOException {
      mapDriver.withInput(new LongWritable(1), new Text(strIn1));
      mapDriver.withInput(new LongWritable(1), new Text(strIn2));
      mapDriver.withOutput(new Text(wordOut1), new IntWritable(1));
      mapDriver.withOutput(new Text(wordOut2), new IntWritable(1));
      mapDriver.withOutput(new Text(wordOut1), new IntWritable(1));

      mapDriver.runTest();
   }

   @Test
   public void testReducer() throws IOException {
      List<IntWritable> values1 = new ArrayList<IntWritable>();
      values1.add(new IntWritable(1));
      values1.add(new IntWritable(1));
      reduceDriver.withInput(new Text(wordOut1), values1);
      reduceDriver.withOutput(new Text(wordOut1), new IntWritable(2));

      List<IntWritable> values2 = new ArrayList<IntWritable>();
      values2.add(new IntWritable(1));
      reduceDriver.withInput(new Text(wordOut2), values2);
      reduceDriver.withOutput(new Text(wordOut2), new IntWritable(1));

      reduceDriver.runTest();
   }

   @Test
   public void testMapReduce() throws IOException {
      mapReduceDriver.withInput(new LongWritable(), new Text(strIn1));
      mapReduceDriver.withInput(new LongWritable(), new Text(strIn2));
      mapReduceDriver.withOutput(new Text(wordOut2), new IntWritable(1));
      mapReduceDriver.withOutput(new Text(wordOut1), new IntWritable(2));

      mapReduceDriver.runTest();
   }


}
