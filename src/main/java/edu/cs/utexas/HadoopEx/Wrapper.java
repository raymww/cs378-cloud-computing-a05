package edu.cs.utexas.HadoopEx;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Writable;

public class Wrapper implements Writable{
    
    DoubleWritable distance;
    DoubleWritable fare_amount;
    DoubleWritable dm;
    DoubleWritable db;
    
    public Wrapper() {
        this.distance = new DoubleWritable();
        this.fare_amount = new DoubleWritable();
    }

    public Wrapper(double distance, double fare_amount) {
        this.distance = new DoubleWritable(distance);
        this.fare_amount = new DoubleWritable(fare_amount);
        // this.multiplied = new DoubleWritable(multiplied);
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        distance.write(dataOutput);
        fare_amount.write(dataOutput);
        // multiplied.write(dataOutput);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        distance.readFields(dataInput);
        fare_amount.readFields(dataInput);
        // multiplied.readFields(dataInput);
    }

    public DoubleWritable getDistance() {
        return this.distance;
    }

    public DoubleWritable getFareAmount() {
        return this.fare_amount;
    }

    // public DoubleWritable getMultiplied() {
    //     return this.multiplied;
    // }

}
