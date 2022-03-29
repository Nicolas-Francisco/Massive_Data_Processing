package org.mdp.hadoop.cli;

import java.io.IOException;
import java.util.ArrayList;

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

public class PairCoStar {

    /**
     * Use this with line.split(SPLIT_REGEX) to get fairly nice
     * word splits.
     */
    public static String SPLIT_REGEX = "\t";

    /**
     * This is the Mapper Class. This sends key-value pairs to different machines
     * based on the key.
     *
     * Remember that the generic is Mapper<InputKey, InputValue, MapKey, MapValue>
     *
     * InputKey we don't care about (a LongWritable will be passed as the input
     * file offset, but we don't care; we can also set as Object)
     *
     * InputKey will be Text: a line of the file
     *
     * MapKey will be Text: a word from the file
     *
     * MapValue will be IntWritable: a count: emit 1 for each occurrence of the word
     *
     * @author Aidan
     *
     */
    public static class PairCoStarMapper extends Mapper<Object, Text, Text, Text>{

        private Text movieKey = new Text();
        private Text star = new Text();


        /**
         * Given the offset in bytes of a line (key) and a line (value),
         * we will output (word,1) for each word in the line.
         *
         * @throws InterruptedException
         *
         */
        @Override
        public void map(Object key, Text value, Context output)
                throws IOException, InterruptedException {
            String line = value.toString();
            String[] rawWord = line.split(SPLIT_REGEX);

            if(rawWord[4].equals("THEATRICAL_MOVIE")) { // si es teatral
                movieKey.set(rawWord[1]+"##"+rawWord[2]+"##"+rawWord[3]);
                star.set(rawWord[0]);
                output.write(movieKey,star);
            }
        }
    }

    /**
     * This is the Reducer Class.
     *
     * This collects sets of key-value pairs with the same key on one machine.
     *
     * Remember that the generic is Reducer<MapKey, MapValue, OutputKey, OutputValue>
     *
     * MapKey will be Text: a word from the file
     *
     * MapValue will be IntWritable: a count: emit 1 for each occurrence of the word
     *
     * OutputKey will be Text: the same word
     *
     * OutputValue will be IntWritable: the final count
     *
     * @author Aidan
     *
     */
    public static class PairCoStarReducer extends Reducer<Text, Text, Text, Text> {

        /**
         * Given a key (a word) and all values (partial counts) for
         * that key produced by the mapper, then we will sum the counts and
         * output (word,sum)
         *
         * @throws InterruptedException
         */
        @Override
        public void reduce(Text key, Iterable<Text> values,
                           Context output) throws IOException, InterruptedException {
            ArrayList<String> valuesArray = new ArrayList<>();
            for(Text value: values) {
                valuesArray.add(value.toString());
            }
            Text pair = new Text();
            for (String actor1 : valuesArray){
                for (String actor2 : valuesArray){
                    if (!actor1.equals(actor2)) {
                        String pairStr = actor1 + "#" + actor2;
                        pair.set(pairStr);
                        output.write(key, pair);
                    }
                }
            }
        }
    }

    /**
     * Main method that sets up and runs the job
     *
     * @param args First argument is input, second is output
     * @throws Exception
     */
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length != 2) {
            System.err.println("Usage: "+CountWords.class.getName()+" <in> <out>");
            System.exit(2);
        }
        String inputLocation = otherArgs[0];
        String outputLocation = otherArgs[1];

        Job job = Job.getInstance(new Configuration());

        FileInputFormat.setInputPaths(job, new Path(inputLocation));
        FileOutputFormat.setOutputPath(job, new Path(outputLocation));

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setMapperClass(PairCoStar.PairCoStarMapper.class);
        job.setReducerClass(PairCoStar.PairCoStarReducer.class);

        job.setJarByClass(PairCoStar.class);
        job.waitForCompletion(true);
    }
}
