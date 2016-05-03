package com.flipkart.mapreduce;

/**
 * Created by dhritiman.das on 4/24/16.
 */
public interface Reducer<KeyIn, ValueIn> {

    void beginReduce(KeyIn key);

    void reduce(ValueIn value);

    void finalizeReduce(KeyIn key);

}
