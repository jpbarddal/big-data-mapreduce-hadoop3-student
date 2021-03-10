package advanced.customwritable;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class ForestFireWritable implements Writable {
    String temp;
    String wind;

    public ForestFireWritable() {}

    public ForestFireWritable(String temp, String wind) {
        this.temp = temp;
        this.wind = wind;
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        temp = in.readUTF();
        wind = in.readUTF();
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(temp);
        out.writeUTF(wind);
    }

    public String toString() {
        return "(" + temp + "," + wind + ")";
    }

}
