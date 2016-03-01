//* Modified WordCount.java *//

import java.io.*;
import java.util.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;

public class InvertedIndex {

    public static void main(String[] args) throws Exception {
        if (args.length < 2) {
            System.err.println("Usage: inputDirectory outputDirectory");
            System.exit(1);
        }

        Job job = Job.getInstance();
        job.setJarByClass(InvertedIndex.class);
        job.setMapperClass(TokenizerMapper.class);
        job.setReducerClass(InvertedIndexReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        boolean showProgress = true;
        System.exit(job.waitForCompletion(showProgress) ? 0 : 1);
    }

    public static class TokenizerMapper
            extends Mapper<Object, Text, Text, Text>{

        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();

        @Override
        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            StringTokenizer itr = new StringTokenizer(value.toString());
            /* Getting the file path */
            String[] filePathString = ((FileSplit) context.getInputSplit()).getPath().toString().split("/");
            while (itr.hasMoreTokens()) {
                word.set(itr.nextToken());
                context.write(word, new Text(filePathString[filePathString.length-1]));
            }
        }
    }

    public static class InvertedIndexReducer
            extends Reducer<Text,Text,Text,Text> {
        private Text result = new Text();

        @Override
        public void reduce(Text key, Iterable<Text> values,
                           Context context
        ) throws IOException, InterruptedException {
            String files = "";
            for (Text val : values) {
                /* Rather than incrementing number of instances
                of a word we append it to the files string
                if it exists in file */

                if (!files.contains(val.toString())){
                  files += val.toString() + " ";
                }
            }
            result.set(files);
            context.write(key, result);
        }
    }

}
