package entropy;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.log4j.BasicConfigurator;
import org.apache.zookeeper.KeeperException;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;

public class FASTAEntropy {

    // https://blogs.msdn.microsoft.com/avkashchauhan/2012/03/29/how-to-chain-multiple-mapreduce-jobs-in-hadoop/
    // https://stackoverflow.com/questions/38111700/chaining-of-mapreduce-jobs

    public static void main(String[] args) throws Exception {
        BasicConfigurator.configure();

        Configuration c = new Configuration();
        String[] files = new GenericOptionsParser(c, args).getRemainingArgs();
        // arquivo de entrada
        Path input = new Path(files[0]);

        // arquivo intermediario
        Path intermediate = new Path("intermediate");

        // Output geral do job final
        Path output = new Path(files[1]);

        // criacao do job e seu nome
        Job j1 = new Job(c, "fasta-count");

        // registro das classes
        j1.setJarByClass(FASTAEntropy.class);
        j1.setMapperClass(MapCount.class);
        j1.setReducerClass(ReduceCount.class);

        // definicao dos tipos de saida
        j1.setOutputKeyClass(Text.class);
        j1.setOutputValueClass(EntropyCounterWritable.class);

        // cadastro dos arquivos de entrada e saida
        FileInputFormat.addInputPath(j1, input);
        FileOutputFormat.setOutputPath(j1, intermediate);

        // lanca o job e aguarda sua execucao
        // System.exit(j1.waitForCompletion(true) ? 0 : 1);
        if(!j1.waitForCompletion(true)){
            System.err.println("Error with Job 1!");
            return;
        }

        Job j2 = new Job(c, "fasta-entropy");

        // registro das classes
        j2.setJarByClass(FASTAEntropy.class);
        j2.setMapperClass(MapEntropy.class);
        j2.setReducerClass(ReduceEntropy.class);

        // definicao dos tipos de saida
        j2.setMapOutputKeyClass(Text.class);
        j2.setMapOutputValueClass(EntropyCounterWritable.class);
        j2.setOutputKeyClass(Text.class);
        j2.setOutputValueClass(FloatWritable.class);

        // arquivos de entrada e saida
        FileInputFormat.addInputPath(j2, intermediate);
        FileOutputFormat.setOutputPath(j2, output);

        if(!j2.waitForCompletion(true)){
            System.err.println("Error with Job 2!");
            return;
        }

    }

    // Gera <entropy, char, qtd, totalchunk>
    public static class MapCount extends Mapper<LongWritable, Text, Text, EntropyCounterWritable> {

        // Funcao de map
        public void map(LongWritable key, Text value, Context con)
                throws IOException, InterruptedException {
            // counters
//            int totalChunk = 0;
            HashMap<String, Integer> counterPerKey = new HashMap<>();

            String line = value.toString();
            if(!line.startsWith(">")) {
                String[] chars = line.split("");
                // para cada palavra
                for (String c : chars) {
                    String cleansed = c.replace("\t", "").replace("\n", "");
                    if(cleansed.length() > 0) {
                        // increments the counter for that specific key
                        if(counterPerKey.containsKey(c)){
                            int newVal = counterPerKey.get(c) + 1;
                            counterPerKey.put(c, newVal);
                        }else{
                            counterPerKey.put(c, 1);
                        }
//                        totalChunk++;
                    }
                }
            }
            for(String k : counterPerKey.keySet()){
                con.write(new Text("entropy"),
                        new EntropyCounterWritable(counterPerKey.get(k), k));
            }
        }
    }

    // Agrupa todos os "entropy" juntos
    // Soma todas as quantidades totais de cada chunk
    // Gera <char, qtd, qtd_total>
    public static class ReduceCount extends Reducer<Text, EntropyCounterWritable, Text, EntropyCounterWritable> {

        // Funcao de reduce
        public void reduce(Text word, Iterable<EntropyCounterWritable> values, Context con)
                throws IOException, InterruptedException {
            int totalChars = 0;

            ArrayList<EntropyCounterWritable> l = new ArrayList<EntropyCounterWritable>();
            for (EntropyCounterWritable i : values) {
                l.add(new EntropyCounterWritable(i));
                //totalChars += i.getTotalChunk();
                totalChars += i.getQtd();
            }

            System.err.println(totalChars);
            for (EntropyCounterWritable v : l) {
                v.setTotalChunk(totalChars);
                con.write(new Text(v.getContent()), new EntropyCounterWritable(v));
            }
        }
    }

    // converte para <char, qtd, qtd_total>
    public static class MapEntropy extends Mapper<LongWritable, Text, Text, EntropyCounterWritable>{
        public void map(LongWritable key, Text value, Context con)
                throws IOException, InterruptedException {
            String v = value.toString();
            String[] splittedVal = v.split("\t");
            String chave = splittedVal[0];

            String strValue = splittedVal[1];

            EntropyCounterWritable vlr = new EntropyCounterWritable(strValue);

            con.write(new Text(chave), vlr);
        }
    }

    // agrupa por char e gera <char, entropia>
    public static class ReduceEntropy extends Reducer<Text, EntropyCounterWritable, Text, FloatWritable>{
        public void reduce(Text key, Iterable<EntropyCounterWritable> values, Context con)
                throws IOException, InterruptedException {

            int totalChar = 0;
            int total = 0;
            for (EntropyCounterWritable v : values){
                totalChar += v.getQtd();
                total = v.getTotalChunk();
            }

            // emitindo entropia
            con.write(key, new FloatWritable(entropy(totalChar, total)));
        }
    }

    public static float entropy(int qtd, int total){
        float p = qtd / (float) total;
        if (p > 0.0 && Float.isFinite(p)){
            return (float) (-1 * p * log2(p));
        }
        return 0;
    }

    public static float log2(double v){
        return (float) (Math.log10(v) / Math.log10(2.0));
    }
}
