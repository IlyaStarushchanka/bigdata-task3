package com.epam.bigdata.mapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.SnappyCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.IOException;
import java.util.Iterator;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by Ilya_Starushchanka on 9/2/2016.
 */
public class VisitsAndSpendsCount {

    public static class CountMapper extends Mapper<LongWritable, Text, Text, VisitsAndSpendsWritable> {
        private final VisitsAndSpendsWritable visitsAndSpendsWritable = new VisitsAndSpendsWritable();
        private Text ip = new Text();

        @Override
        protected void map(LongWritable key, Text value,
                           Context context)
                throws IOException, InterruptedException {
            String line = value.toString();
            String[] columns = line.split("\\s+");
            Pattern p = Pattern.compile("(\\d+[.]){3,}(\\d+|\\*)");
            Matcher m = p.matcher(line);

            if (m.find()){
                String ipStr = m.group();
                ip.set(ipStr);
                visitsAndSpendsWritable.setVisitsCount(1);
                visitsAndSpendsWritable.setSpendsCount(Integer.parseInt(columns[columns.length - 4]));
                context.write(ip, visitsAndSpendsWritable);
            }
        }
    }

    public static class CountReduce extends Reducer<Text, VisitsAndSpendsWritable, Text, VisitsAndSpendsWritable>{
        private VisitsAndSpendsWritable result = new VisitsAndSpendsWritable();

        @Override
        protected void reduce(Text ip, Iterable<VisitsAndSpendsWritable> values, Context context) throws IOException, InterruptedException {
            int sumPrice = 0;
            int sumVisits = 0;
            for (VisitsAndSpendsWritable val : values) {
                sumPrice += val.getSpendsCount();
                sumVisits += val.getVisitsCount();
            }
            result.setVisitsCount(sumVisits);
            result.setSpendsCount(sumPrice);
            context.write(ip, result);
        }

    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length < 2) {
            System.err.println("Usage: VisitsAndSpendsCount <in> <out> [<in>...]");
            System.exit(2);
        }
        Job job = new Job(conf, "VisitsAndSpendsCount");
        job.setJarByClass(TagsCount.class);

        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));

        job.setMapperClass(CountMapper.class);
        job.setCombinerClass(CountReduce.class);
        job.setReducerClass(CountReduce.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(VisitsAndSpendsWritable.class);


        FileOutputFormat.setCompressOutput(job, true);
        FileOutputFormat.setOutputCompressorClass(job, SnappyCodec.class);
        //job.setOutputFormatClass(SequenceFileOutputFormat.class);

        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
        //SequenceFileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
        //SequenceFileOutputFormat.setOutputCompressionType(job, SequenceFile.CompressionType.BLOCK);

        boolean result = job.waitForCompletion(true);

        /*System.out.println("Browsers Counter :");
        for (Counter counter : job.getCounters().getGroup(Browser.class.getCanonicalName())) {
            System.out.println(" - " + counter.getDisplayName() + ": " + counter.getValue());
        }*/

        System.exit(result ? 0 : 1);
    }

}
