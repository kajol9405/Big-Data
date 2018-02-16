
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class TopTenMutualFriends {
    public static class Map
    extends Mapper<LongWritable, Text, Text, Text>{

        private Text user_key = new Text(); // type of output key
        private Text friends_value = new Text();
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            //key: byte-offset of line.
            //value: the line in question.
            String[] user_friends_data = value.toString().split("\t");
            String[] friends;
            if(user_friends_data.length>1){
               
            friends=user_friends_data[1].split(",");
            String[] users = new String[2];
            for(String friend : friends){
               
                users[0]=user_friends_data[0];
                users[1]=friend;
           
                Long user_0 = Long.parseLong(users[0]);
                Long user_1 = Long.parseLong(users[1]);
                if(user_0 < user_1){
                   
                    user_key.set(users[0]+","+users[1]);
                }
                else{
                    user_key.set(users[1]+","+users[0]);
                }
               
                friends_value.set(user_friends_data[1]);
               
                context.write(user_key, friends_value);
               
            }
        }       
    }
}
       
      public static class Reduce
      extends Reducer<Text,Text,Text,IntWritable> {
         
          private Text result = new Text();
          private HashMap<Text, IntWritable> countMap = new HashMap<Text,IntWritable>();
         
          public void reduce(Text key,Iterable<Text> values,Context context) throws IOException, InterruptedException{
              HashSet<String> set1 = new HashSet<>();
                         
              String[] value_string = new String[2];
              int i=0;
              for(Text value : values){
                  value_string[i] =value.toString();
                  i++;
              }
              String[] values_s1 = value_string[0].split(",");
              String[] values_s2 = value_string[1].split(",");
             
              for(String split : values_s1){
                  set1.add(split); 
              }
              String result_string=" ";
              int friends_count = 0;
              for(String split : values_s2){
                  if(set1.contains(split)) {
                             result_string = result_string + split +",";
                             friends_count++;    
                  }
              }
            
              result.set(result_string);
              countMap.put(new Text(key),new IntWritable(friends_count));
          }
         
          @Override
            protected void cleanup(Context context) throws IOException, InterruptedException {

                java.util.Map<Text, IntWritable> sortedMap = SortMapFunction.sortByValues(countMap);

                int counter = 0;
                for (Text key : sortedMap.keySet()) {
                    if (counter++ ==10 ) {
                        break;
                    }
                    context.write(key,(sortedMap.get(key)) );
                }
            }
      }
      
     
   
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        // get all args
        if (otherArgs.length != 2) {
            System.err.println("Usage: TopTenMutualFriends <in> <out>");
            System.exit(2);
        }


        // create a job with name "TopTenMutualFriends"
        @SuppressWarnings("deprecation")
        Job job = new Job(conf, "TopTenMutualFriends");
        job.setJarByClass(TopTenMutualFriends.class);
        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);

        // set output key type
        job.setOutputKeyClass(Text.class);
        // set output value type
        job.setOutputValueClass(Text.class);
        //set the HDFS path of the input data
        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        // set the HDFS path for the output
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
        //Wait till job completion
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
