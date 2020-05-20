package com.wikibook.bigdata.smartcar.mahout;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
 
public class TextToSequence 
{
    public static void main( String[] args ) throws Exception
    {
            if(args.length !=2 ){
                    System.err.println("Usage : Sequence File Writer Utility <input path> <output path>");
                    System.exit(-1);
            }
            Configuration conf = new Configuration();
            Job job = new Job(conf);
            job.setJarByClass(TextToSequence.class);
            job.setJobName("SequenceFileWriter");
             
            FileInputFormat.addInputPath(job,new Path(args[0]) );
            FileOutputFormat.setOutputPath(job, new Path(args[1]));
             
            job.setMapperClass(FormatConverterMapper.class);
             
            //job.setInputFormatClass(KeyValueTextInputFormat.class);
            job.setOutputFormatClass(SequenceFileOutputFormat.class);
             
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);
            job.setNumReduceTasks(0);
             
             
            System.exit(job.waitForCompletion(true) ? 0:1);
    }
}


