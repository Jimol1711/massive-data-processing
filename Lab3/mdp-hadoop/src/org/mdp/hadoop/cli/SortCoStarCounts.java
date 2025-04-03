package org.mdp.hadoop.cli;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

/**
 * Java class to sort the co-stars count pairs.
 *
 * This is adapted from SortWordCounts to work with the output of CountCoStars.
 *
 */
public class SortCoStarCounts {

    /**
     * This is the Mapper Class. This sends key-value pairs to different machines
     * based on the key.
     *
     * InputKey will be the offset in the file (long), and InputValue will be
     * a line with the co-star pair and its count.
     *
     * MapKey will be IntWritable: the count for the co-stars.
     *
     * MapValue will be Text: the co-star pair.
     *
     *
     */
    public static class SortCoStarCountsMapper extends
            Mapper<Object, Text, DescendingIntWritable, Text>{

        /**
         * @throws InterruptedException
         *
         * Each input line should be as follows:
         *
         * actor1##actor2[\t]count
         *
         * Parse this and map count as key, co-star pair as value.
         *
         * Note DescendingIntWritable, which offers
         * 	inverse sorting (largest first!).
         *
         */
        @Override
        public void map(Object key, Text value, Context output)
                throws IOException, InterruptedException {
            String[] split = value.toString().split("\t");
            output.write(new DescendingIntWritable(Integer.parseInt(split[1])), new Text(split[0]));
        }
    }

    /**
     * This is the Reducer Class.
     *
     * This collects sets of key-value pairs with the same key on one machine.
     *
     * MapKey will be DescendingIntWritable: the co-star count.
     *
     * MapValue will be Text: the co-star pair.
     *
     * OutputKey will be Text: the co-star pair.
     *
     * OutputValue will be IntWritable: the count of the co-stars (sorted this time!).
     *
     *
     */
    public static class SortCoStarCountsReducer
            extends Reducer<DescendingIntWritable, Text, Text, IntWritable> {

        /**
         * @throws InterruptedException
         *
         * The keys (counts) are called in descending order ...
         * ... so for each value (co-star pair) of a key, we just write
         * (value,key) pairs to the output and we're done.
         *
         */
        @Override
        public void reduce(DescendingIntWritable key, Iterable<Text> values,
                           Context output) throws IOException, InterruptedException {
            for(Text value : values) {
                output.write(value, key);  // Emit the co-star pair and its count
            }
        }
    }

    /**
     * Main method that sets up and runs the job.
     *
     * @param args First argument is input, second is output
     * @throws Exception
     */
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length != 2) {
            System.err.println("Usage: "+SortCoStarCounts.class.getName()+" <in> <out>");
            System.exit(2);
        }
        String inputLocation = otherArgs[0];
        String outputLocation = otherArgs[1];

        Job job = Job.getInstance(new Configuration());

        // Set the key and value types for input/output
        job.setMapOutputKeyClass(DescendingIntWritable.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        // Set Mapper and Reducer classes
        job.setMapperClass(SortCoStarCountsMapper.class); // no combiner this time!
        job.setReducerClass(SortCoStarCountsReducer.class);

        // Set the input and output file paths
        FileInputFormat.setInputPaths(job, new Path(inputLocation));
        FileOutputFormat.setOutputPath(job, new Path(outputLocation));

        // Set the Jar class
        job.setJarByClass(SortCoStarCounts.class);

        // Wait for the job to complete
        job.waitForCompletion(true);
    }

    /**
     * A class that inverts the order for IntWritable objects so
     * we can do a descending order.
     *
     */
    public static class DescendingIntWritable extends IntWritable {

        public DescendingIntWritable() {}

        public DescendingIntWritable(int val) {
            super(val);
        }

        public int compareTo(IntWritable o) {
            return -super.compareTo(o);  // Inverse comparison for descending order
        }
    }
}