import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;


public class TopTenBusiness {
	public static class Reviews_Mapper extends Mapper<Object, Text, Text, FloatWritable> {

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
         	
        	String[] reviews_data = value.toString().split("::"); 
        	
            Float rating = Float.parseFloat(reviews_data[3]);
            
            Text reviews_key = new Text(reviews_data[2]);
            FloatWritable reviews_values = new FloatWritable(rating);
            
            context.write(reviews_key, reviews_values);  
        }
    }

	public static class Business_Mapper extends Mapper<Object, Text, Text, Text> {
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            
			String[] business_data = value.toString().split("::");
			String business_key = business_data[0];
			String business_value = "Business" + "\t" + business_data[1] + "\t" + business_data[2];
            
            context.write(new Text(business_key), new Text(business_value));  
        }
	}
	public static class Map_ratings_business extends Mapper<Object, Text, Text, Text> {
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            
			String[] data = value.toString().split("\t");
			String business_id = data[0];
			String average_ratings = data[1];
            
            context.write(new Text(business_id), new Text(average_ratings));  
        }
		
	}
    public static class Reduce extends Reducer<Text,FloatWritable,Text,FloatWritable> {
    	
    	private Map<Text, FloatWritable> business_ratings_map = new HashMap<Text, FloatWritable>();
        
    	public void reduce(Text key, Iterable<FloatWritable> values, Context context) throws IOException, InterruptedException {
        	float total_reviews = 0;
        	int num_ratings = 0;
        	
        	for(FloatWritable value : values) {
        		total_reviews = total_reviews + value.get();
        		num_ratings++;
        	}
        	float averageRating = total_reviews/num_ratings;
        	
        	business_ratings_map.put(new Text(key), new FloatWritable(averageRating));
        }
        
        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
        	
        		Map<Text, FloatWritable> sort_ratings = SortMapFunction.sortByValues(business_ratings_map);
        		int topten = 0;
                for (Text key : sort_ratings.keySet()) {
                    if (topten++ == 10) {
                        break;
                    }      
                    context.write(key, sort_ratings.get(key));
                }
        }
        
    }

    public static class Reduce_Side_Join_Reducer extends Reducer<Text, Text, Text, Text> {
    	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
    		String avg_ratings = "";
    		String business_information = "";
    		boolean flag = false;
    		for(Text value : values) {
    			String data = value.toString();
    			if(data.contains("Business")) {
    				data = data.replace("Business", "");
    				business_information = data;
    			} else {
    				avg_ratings = data;
    				flag = true;
    			}
    		}
    		if(!flag)
    			return;
    	
    		context.write(new Text(key), new Text(business_information + "\t" + avg_ratings));
    	}
    }
    // Driver program
    public static void main(String[] args) throws Exception {
    	   Configuration conf = new Configuration();
           String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
           if (otherArgs.length != 4) {
               System.err.println("Usage: TopTenBusiness <input1> <input2> <intermideatePath> <output>");
               System.exit(2);
           }
            
           Job job1 = Job.getInstance(conf, "TopTenBusiness");
           job1.setJarByClass(TopTenBusiness.class);
    
           job1.setMapperClass(Reviews_Mapper.class);
           job1.setReducerClass(Reduce.class);
    
           job1.setMapOutputKeyClass(Text.class);
           job1.setMapOutputValueClass(FloatWritable.class);
    
           job1.setOutputKeyClass(Text.class);
           job1.setOutputValueClass(FloatWritable.class);
           job1.setNumReduceTasks(1);
    
           Path reviewPath = new Path(otherArgs[0]);
           Path businessPath = new Path(otherArgs[1]);
           Path intermediatePath = new Path(otherArgs[2]);
           Path outputPath = new Path(otherArgs[3]);
    
           FileInputFormat.addInputPath(job1, reviewPath);
           FileOutputFormat.setOutputPath(job1, intermediatePath);
           job1.waitForCompletion(true);
    
           Job job2 = Job.getInstance(conf, "TopTenBusiness");
           job2.setJarByClass(TopTenBusiness.class);
           MultipleInputs.addInputPath(job2, businessPath, TextInputFormat.class, Business_Mapper.class);
           MultipleInputs.addInputPath(job2, intermediatePath, TextInputFormat.class, Map_ratings_business.class);
    
           job2.setReducerClass(Reduce_Side_Join_Reducer.class);
    
           job2.setOutputKeyClass(Text.class);
           job2.setOutputValueClass(Text.class);
           FileInputFormat.setMinInputSplitSize(job2, 500000000);
    
           FileSystem hdfsFS = FileSystem.get(conf);
           if (hdfsFS.exists(outputPath)) {
               hdfsFS.delete(outputPath, true);
           }
           FileOutputFormat.setOutputPath(job2, outputPath);
    
           job2.waitForCompletion(true);
    
           hdfsFS.delete(intermediatePath, true);
    }

}
