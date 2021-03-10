package entropy;

import java.io.*;
import java.util.HashMap;
import java.util.zip.CheckedOutputStream;

public class EntropyTester {

    public static void main (String args[]){
        try {

            File f = new File("/Users/jeanpaul/Desktop/NC_000001.fasta");

            BufferedReader b = new BufferedReader(new FileReader(f));

            String readLine = "";

            System.out.println("Reading...");

            int total = 0;
            HashMap<String, Integer> counter = new HashMap<>();

            // ignoring header
            readLine = b.readLine();

            while ((readLine = b.readLine()) != null) {
                //System.out.println(readLine);

                for(String s : readLine.split("")){
                    s = s.replace("\t", "").replace("\n", "");
                    if (s.length() > 0) {
                        if (counter.containsKey(s)) {
                            int newval = counter.get(s) + 1;
                            counter.put(s, newval);
                        } else {
                            counter.put(s, 1);
                        }
                        total++;
                    }
                }
            }

            System.out.println("total = " + total);
            for(String s : counter.keySet()){
                System.out.println(s + "\t" + counter.get(s));
            }

            for(String s : counter.keySet()){
                System.out.println(s + "\t" + entropy(counter.get(s), total));
            }

        } catch (IOException e) {
            e.printStackTrace();
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
