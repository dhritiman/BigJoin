package com.flipkart.mapreduce;

/**
 * Created by dhritiman.das on 4/24/16.
 */
public interface PartitionAwareIterator<T> {

    boolean hasNext();

    T next();
}
