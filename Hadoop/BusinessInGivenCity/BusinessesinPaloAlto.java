import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.HashSet;
import java.util.Set;
 
import org.apache.hadoop.conf.Configuration;
 
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
 
import org.apache.hadoop.io.Text;
 
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
 
public class BusinessesinPaloAlto {
 
    public static class MapFilter extends Mapper<Object, Text, Text, Text> {
        Set<String> business_ids = new HashSet<String>();
        String city_name;
 
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
 
            String[] data = value.toString().split("::");
            String userId = data[1];
            String businessId = data[2];
            String rating = data[3];
              
            if (business_ids.contains(businessId)) {
                context.write(new Text(userId.toString()), new Text(rating));
            }
        }
        
        public void setup(Context context) throws IOException, InterruptedException {
            
            Configuration conf = context.getConfiguration();
            city_name = conf.get("input");
             
            try {
                URI[] cacheFile = context.getCacheFiles();
                URI uri = cacheFile[0];
                FileReader file = new FileReader(new File(uri.getPath()).getName());
                BufferedReader bufferedReader = new BufferedReader(file);
                String file_input = bufferedReader.readLine();
                while (file_input != null) {
                    if (file_input.contains(city_name)) {
                        business_ids.add(file_input.split("::")[0]);
                    }
                    file_input = bufferedReader.readLine();
                }
                bufferedReader.close();
            } catch (Exception e) {
                System.out.println(e.getMessage());
            }
        }
    }
 
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
         
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length != 3) {
            System.err.println("Usage: BusinessesinPaloAlto <in1> <in2> <out>");
            System.exit(2);
        }
        conf.set("input", "Palo Alto");
         
        Job job = Job.getInstance(conf, "BusinessesinPaloAlto");
        job.setJarByClass(BusinessesinPaloAlto.class);
 
        job.setMapperClass(MapFilter.class);
 
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
 
        Path inputFile = new Path(otherArgs[0]);
        Path cacheFile = new Path(otherArgs[1]);
        Path outputFile = new Path(otherArgs[2]);
 
        FileInputFormat.addInputPath(job, inputFile);
 
        job.addCacheFile(cacheFile.toUri());
 
        FileSystem hdfsFS = FileSystem.get(conf);
        if (hdfsFS.exists(outputFile)) {
            hdfsFS.delete(outputFile, true);
        }
        FileOutputFormat.setOutputPath(job, outputFile);
 
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}