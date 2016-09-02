package com.epam.bigdata.mapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by Ilya_Starushchanka on 9/2/2016.
 */
public class TagsCount {

    public static class WordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
        private final IntWritable one = new IntWritable(1);
        private Text word = new Text();
        private Set stopWords = new HashSet();

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            try{
                Path[] stopWordsFiles = DistributedCache.getLocalCacheFiles(context.getConfiguration());
                if(stopWordsFiles != null && stopWordsFiles.length > 0) {
                    for(Path stopWordFile : stopWordsFiles) {
                        readFile(stopWordFile);
                    }
                }
            } catch(IOException ex) {
                System.err.println("Exception in mapper setup: " + ex.getMessage());
            }
        }

        @Override
        protected void map(LongWritable key, Text value,
                           Context context)
                throws IOException, InterruptedException {

            String line = value.toString();
            if (isLineValid(line)){
                String[] params = line.split("\\s+");
                String[] tags = params[1].toUpperCase().split(",");
                for (String tag : tags){
                    if(!stopWords.contains(tag)) {
                        word.set(tag);
                        context.write(word,one);
                    }
                }
            }
        }

        public boolean isLineValid(String line){
            Pattern p = Pattern.compile("\\d+\\s+[\\w\\d,]+[\\s\\w]+http[s]*:[^\\s\\r\\n]+");
            Matcher m = p.matcher(line);
            return m.matches();
        }

        private void readFile(Path filePath) {
            try{
                BufferedReader bufferedReader = new BufferedReader(new FileReader(filePath.toString()));
                String stopWord = null;
                while((stopWord = bufferedReader.readLine()) != null) {
                    stopWords.add(stopWord.toLowerCase());
                }
            } catch(IOException ex) {
                System.err.println("Exception while reading stop words file: " + ex.getMessage());
            }
        }

    }

    public static class WordCountReduce extends Reducer<Text, IntWritable, Text, IntWritable>{

        private IntWritable result = new IntWritable();

        @Override
        protected void reduce(Text word, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            Iterator<IntWritable> itr = values.iterator();
            while (itr.hasNext()){
                sum += itr.next().get();
            }
            result.set(sum);
            context.write(word, result);
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length < 2) {
            System.err.println("Usage: tagscount <in> <out> [<in>...]");
            System.exit(2);
        }
        Job job = new Job(conf, "TagsCount");
        job.setJarByClass(TagsCount.class);

        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));

        job.setMapperClass(WordCountMapper.class);
        job.setCombinerClass(WordCountReduce.class);
        job.setReducerClass(WordCountReduce.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        if (otherArgs.length > 2) {
            DistributedCache.addCacheFile(new Path(otherArgs[2]).toUri(), job.getConfiguration());
        }

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

}
