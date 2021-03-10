package advanced.customwritable;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.io.Text;
import org.apache.log4j.BasicConfigurator;
import java.io.IOException;

public class ForestFire {

    public static void main(String[] args) throws Exception {
        BasicConfigurator.configure();

        Configuration c = new Configuration();
        String[] files = new GenericOptionsParser(c, args).getRemainingArgs();

        // arquivo de entrada
        Path inputA = new Path(files[0]);
        Path inputB = new Path(files[1]);

        // arquivo de saida
        Path output = new Path(files[2]);

        // criacao do job e seu nome
        Job j = new Job(c, "forestfire-professor");

        j.setJarByClass(ForestFire.class);
        j.setMapperClass(ForestFireMapper.class);
        j.setReducerClass(ForestFireReducer.class);
        j.setOutputKeyClass(Text.class);
        j.setOutputValueClass(ForestFireWritable.class);
        FileInputFormat.addInputPath(j, inputA);
        FileInputFormat.addInputPath(j, inputB);
        FileOutputFormat.setOutputPath(j, output);

        System.exit(j.waitForCompletion(true) ? 0 : 1);
    }

    public class ForestFireMapper extends Mapper<Object, Text, Text, ForestFireWritable> {

        private Text month = new Text();
        private Text temp = new Text();
        private Text wind = new Text();

        public void map(Object key, Text value, Context context) throws IOException,
                InterruptedException {

            String[] str_array = value.toString().split(",");
            month.set(str_array[2]);
            temp.set(str_array[12]);
            wind.set(str_array[10]);

            String monthUpper = month.toString().toUpperCase();
            context.write(new Text(monthUpper),
                    new ForestFireWritable(temp.toString(), wind.toString()));

        }
    }

    public class ForestFireReducer extends Reducer<Text, ForestFireWritable, Text, ForestFireWritable> {

        public void reduce(Text key,
                           Iterable<ForestFireWritable> values,
                           Context context) throws IOException, InterruptedException {
            float maxWind = 0.0f;
            float maxTemp = 0.0f;

            for (ForestFireWritable val : values) {
                float tempValue = Float.parseFloat(val.temp);
                float windValue = Float.parseFloat(val.wind);

                // Max wind
                if(windValue > maxWind){
                    maxWind = windValue;
                }

                // Max temp
                if(tempValue > maxTemp){
                    maxTemp = tempValue;
                }
            }

            context.write(key, new ForestFireWritable(Float.toString(maxTemp),Float.toString(maxWind)));
        }
    }

}
