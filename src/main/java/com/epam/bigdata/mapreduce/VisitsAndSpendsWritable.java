package com.epam.bigdata.mapreduce;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by Ilya_Starushchanka on 9/2/2016.
 */
public class VisitsAndSpendsWritable  {

    private int visitsCount;
    private int spendsCount;

    public VisitsAndSpendsWritable(){}

    public VisitsAndSpendsWritable(int visitsCount, int spendsCount){
        this.visitsCount = visitsCount;
        this.spendsCount = spendsCount;
    }

    public void readFields(DataInput in) throws IOException {
        this.visitsCount = in.readInt();
        this.spendsCount = in.readInt();
    }

    public void write(DataOutput out) throws IOException {
        out.writeInt(this.visitsCount);
        out.writeInt(this.spendsCount);
    }

    public int getVisitsCount() {
        return visitsCount;
    }

    public void setVisitsCount(int visitsCount) {
        this.visitsCount = visitsCount;
    }

    public int getSpendsCount() {
        return spendsCount;
    }

    public void setSpendsCount(int spendsCount) {
        this.spendsCount = spendsCount;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof VisitsAndSpendsWritable)) return false;

        VisitsAndSpendsWritable that = (VisitsAndSpendsWritable) o;

        if (getVisitsCount() != that.getVisitsCount()) return false;
        return getSpendsCount() == that.getSpendsCount();

    }

    @Override
    public int hashCode() {
        int result = getVisitsCount();
        result = 31 * result + getSpendsCount();
        return result;
    }

    @Override
    public String toString() {
        return "VisitsAndSpendsWritable{" +
                "visitsCount=" + visitsCount +
                ", spendsCount=" + spendsCount +
                '}';
    }

}
