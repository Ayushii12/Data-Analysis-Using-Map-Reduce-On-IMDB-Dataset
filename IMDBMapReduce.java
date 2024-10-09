// CSE 5331-001: Project 3
// Team no. 3
// Reference WordCount.java from MapReduce example provided as part of the project setup

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class IMDBMapReduce {
  public static class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable> {
    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
      FileSplit fileSplit = (FileSplit) context.getInputSplit();
      String filename = fileSplit.getPath().getName();

      try {
        String[] tokens = value.toString().split(";");
        if (tokens.length == 6) {
          String period = "";
          String genreTag = "";

          String yeaString = tokens[3];
          String type = tokens[1];
          Double rating = Double.parseDouble(tokens[4]);
          String[] genres = tokens[5].split(",");

          String actionThrillerTag = "Action;Thriller";
          String adventureDramaTag = "Adventure;Drama";
          String comedyRomanceTag = "Comedy;Romance";

          if (yeaString.equals("\\N") || rating < 7.5 || genres.length < 2 || !type.equals("movie")) {
            return;
          }

          Integer year = Integer.parseInt(tokens[3]);
          List<String> genresList = Arrays.asList(genres);

          if (genresList.contains("Action") && genresList.contains("Thriller")) {
            genreTag = actionThrillerTag;
          } else if (genresList.contains("Adventure") && genresList.contains("Drama")) {
            genreTag = adventureDramaTag;
          } else if (genresList.contains("Comedy") && genresList.contains("Romance")) {
            genreTag = comedyRomanceTag;
          } else {
            return;
          }

          if (year >= 1991 && year <= 2000) {
            period = "[1991-2000]";
          } else if (year >= 2001 && year <= 2010) {
            period = "[2001-2010]";
          } else if (year >= 2011 && year <= 2020) {
            period = "[2011-2020]";
          }

          if (period != "" && genreTag != "") {
            word.set(period + "," + genreTag);
            context.write(word, one);
          } else {
            return;
          }
        } else {
          return;
        }
      } catch (NumberFormatException e) {
        return;
      }
    }
  }

  public static class IntSumReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
    private IntWritable result = new IntWritable();

    public void reduce(Text key, Iterable<IntWritable> values, Context context)
        throws IOException, InterruptedException {
      int sum = 0;
      for (IntWritable val : values) {
        sum += val.get();
      }

      result.set(sum);
      context.write(key, result);
    }
  }

  // Main method - Setup MapReduce job configuration and file input/output paths
  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "IMDB_Count");
    job.setJarByClass(IMDBMapReduce.class);
    job.setMapperClass(TokenizerMapper.class);
    job.setCombinerClass(IntSumReducer.class);
    job.setReducerClass(IntSumReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
