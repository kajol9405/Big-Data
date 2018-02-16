import java.io.IOException;
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

public class MutualFriends {
    public static class Map
    extends Mapper<LongWritable, Text, Text, Text>{

        private Text user_key = new Text(); 
        private Text friends_value = new Text();
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
         
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
      extends Reducer<Text,Text,Text,Text> {
         
          private Text result = new Text();
          private Text result_key = new Text();
          private Text result_key_1 = new Text();
          private Text result_key_2 = new Text();
          private Text result_key_3 = new Text();
          private Text result_key_4 = new Text();
          private Text result_key_5 = new Text();
          HashSet<Text> key_set = new HashSet<>();
          
         
          public void reduce(Text key,Iterable<Text> values,Context context) throws IOException, InterruptedException{
              HashSet<String> set1 = new HashSet<>();
              
              //uncomment the below 3 lines to run from command  line
              
//              Configuration conf= context.getConfiguration();
//              String param = conf.get("key");            
 //             result_key.set(param);
              
              result_key_1.set("0,4");
              result_key_2.set("20,22939");
              result_key_3.set("1,29826");
              result_key_4.set("6222,19272");
              result_key_5.set("28041,28056");
              
              key_set.add(result_key);
             
              key_set.add(result_key_1);
              key_set.add(result_key_2);
              key_set.add(result_key_3);
              key_set.add(result_key_4);
              key_set.add(result_key_5);
              
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
         
              for(String split : values_s2){
                  if(set1.contains(split)) {
           
                             result_string = result_string + split + ",";
          
                  }
              }
              
              if(result_string.length() > 0) {
            	  result_string = result_string.substring(0, result_string.length() - 1);
              }
            
              if(key_set.contains(key)){
                    result.set(result_string);
                    context.write(key, result);
             }
          }
         
      }
   
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        
        //uncomment the below code to run for command line
        //conf.set("key", (args[2]));
        
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        
        // get all args
        if (otherArgs.length != 2) {
          System.err.println("Usage: MutualFriends <in> <out>");
          System.exit(2);
      }
        
        //uncomment the below code to run for command line
//        if (otherArgs.length != 3) {
//            System.err.println("Usage: MutualFriends <in> <out>");
//            System.exit(2);
//        }


        // create a job with name "mutualfriends"
        @SuppressWarnings("deprecation")
        Job job = new Job(conf, "mutualfriends");
        job.setJarByClass(MutualFriends.class);
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