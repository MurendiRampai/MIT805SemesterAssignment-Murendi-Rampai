package SalesPerWday;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;

import javax.naming.Context;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class SalesPerWday {

    public static class Mapper1 extends Mapper<Object, Text, Text, Text> {

        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] line = value.toString().split(",");
            String wday = line[11];
            String price = line[5];

            if (line[2].equals("purchase")) {if (price != "price") {
            context.write(new Text(wday), new Text(price));
        }
            }
        }
    }

    public static class Reducer1 extends Reducer<Text, Text, Text, Text> {

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            double total = 0.0;
            

            for (Text value : values) {
                total += Double.parseDouble(value.toString());
                
            }

            double wdaySales = total;

            context.write(key, new Text(String.valueOf(wdaySales)));
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "wdaySales");

        job.setJarByClass(SalesPerWday.class);
        job.setMapperClass(Mapper1.class);
        job.setReducerClass(Reducer1.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        TextInputFormat.addInputPath(job, new Path(args[0]));
        TextOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}

