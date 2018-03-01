package com.yuhans.bigdata.hadoop.logparser.writableentities;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;

/**
 * Custom Writable entity is used for more comfortably way of saving output.
 * It keeps number of total bytes and average bytes per request.
 * Mapper and Combiner use 'average' field to save number of requests, and Reducer then use it to count
 * average bytes and write them to this field.
 *
 * @author Artem_Iushin <Artem_Iushin@epam.com>
 */
public class IpStats implements Writable {

    private double average;
    private int totalBytes;

    public IpStats() {}

    public IpStats(double average, int totalBytes) {
        this.average = average;
        this.totalBytes = totalBytes;
    }

    public void write(DataOutput out) throws IOException {
        out.writeDouble(average);
        out.writeInt(totalBytes);
    }

    public void readFields(DataInput in) throws IOException {
        average = in.readDouble();
        totalBytes = in.readInt();
    }

    public double getAverage() {
        return average;
    }

    public void setAverage(double average) {
        this.average = average;
    }

    public int getTotalBytes() {
        return totalBytes;
    }

    public void setTotalBytes(int totalBytes) {
        this.totalBytes = totalBytes;
    }

    @Override
    public String toString() {
        return average + ", " + totalBytes;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        IpStats ipStats = (IpStats) o;
        return Double.compare(ipStats.average, average) == 0 &&
                totalBytes == ipStats.totalBytes;
    }

    @Override
    public int hashCode() {
        return Objects.hash(average, totalBytes);
    }
}
