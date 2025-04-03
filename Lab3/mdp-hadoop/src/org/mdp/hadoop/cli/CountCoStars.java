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
 * Java class to count co-star pairs based on their appearance in the same movie.
 */
public class CountCoStars {

    /**
     * This is the Mapper Class. This sends key-value pairs to different machines
     * based on the key.
     *
     * InputKey: Text (actor pair, e.g., "Pavel Abdalov##Yuliya Silaeva")
     * MapValue: IntWritable (the count, which is 1 for each pair)
     */
    public static class CountCoStarsMapper extends Mapper<Object, Text, Text, IntWritable> {

        private final IntWritable one = new IntWritable(1);
        private Text actorPair = new Text();

        /**
         * Given the key-value input of actor pairs and their counts, output each pair
         * with a value of 1.
         */
        @Override
        public void map(Object key, Text value, Context output)
                        throws IOException, InterruptedException {
            // The value is in the format: "Actor1##Actor2 1"
            String[] parts = value.toString().split("\t");

            // The key is the actor pair, and the value should be 1
            actorPair.set(parts[0]);
            output.write(actorPair, one);
        }
    }

    /**
     * This is the Reducer Class. It collects sets of key-value pairs with the same
     * key on one machine and sums the counts.
     *
     * MapKey: Text (actor pair, e.g., "Pavel Abdalov##Yuliya Silaeva")
     * MapValue: IntWritable (the count for that pair, e.g., 1)
     * OutputKey: Text (the actor pair)
     * OutputValue: IntWritable (the total count of co-star appearances)
     */
    public static class CountCoStarsReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

        /**
         * Given a key (actor pair) and all values (partial counts) for that key
         * produced by the mapper, sum the counts and output (actorPair, totalCount).
         */
        @Override
        public void reduce(Text key, Iterable<IntWritable> values, Context output)
                throws IOException, InterruptedException {
            int totalCount = 0;

            // Sum up the counts for the actor pair
            for (IntWritable value : values) {
                totalCount += value.get();
            }

            // Write the result (actor pair and total count)
            output.write(key, new IntWritable(totalCount));
        }
    }

    /**
     * Main method that sets up and runs the job
     *
     * @param args First argument is input, second is output
     */
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length != 2) {
            System.err.println("Usage: " + CountCoStars.class.getName() + " <in> <out>");
            System.exit(2);
        }
        String inputLocation = otherArgs[0];
        String outputLocation = otherArgs[1];

        Job job = Job.getInstance(new Configuration());

        FileInputFormat.setInputPaths(job, new Path(inputLocation));
        FileOutputFormat.setOutputPath(job, new Path(outputLocation));

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setMapperClass(CountCoStarsMapper.class);
        job.setCombinerClass(CountCoStarsReducer.class);
        job.setReducerClass(CountCoStarsReducer.class);

        job.setJarByClass(CountCoStars.class);
        job.waitForCompletion(true);
    }
}
