package com.flipkart.mapreduce;

import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * Created by dhritiman.das on 4/24/16.
 */
public interface Context<K,V> {

    void emit(K key, V value);

    void mapFinished();

    ConcurrentLinkedQueue getQueueForReducer(K key);

    boolean allMapFinished();
}
