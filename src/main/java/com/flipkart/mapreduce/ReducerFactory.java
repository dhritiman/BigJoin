package com.flipkart.mapreduce;

/**
 * Created by dhritiman.das on 4/26/16.
 */
public interface ReducerFactory<K,V> {

    Reducer<K,V> newReducer();
}
