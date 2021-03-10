package advanced.customwritable;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;

public class FireAvgTempWritable implements WritableComparable<FireAvgTempWritable> {

    private int n;
    private float temp;

    public FireAvgTempWritable() {
    }

    public FireAvgTempWritable(int n, float temp) {
        this.n = n;
        this.temp = temp;
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        temp = Float.parseFloat(in.readUTF());
        n = Integer.parseInt(in.readUTF());
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(String.valueOf(temp));
        out.writeUTF(String.valueOf(n));
    }

    public int getN() {
        return n;
    }

    public float getTemp() {
        return temp;
    }

    public void setN(int n) {
        this.n = n;
    }

    public void setTemp(float temp) {
        this.temp = temp;
    }

    public String toString() {
        return "(" + temp + "," + n + ")";
    }

    @Override
    public int compareTo(FireAvgTempWritable o) {
        if(this.hashCode() < o.hashCode()){
            return -1;
        }else if(this.hashCode() > o.hashCode()){
            return +1;
        }
        return 0;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        FireAvgTempWritable that = (FireAvgTempWritable) o;
        return n == that.n && Float.compare(that.temp, temp) == 0;
    }

    @Override
    public int hashCode() {
        return Objects.hash(n, temp);
    }
}
