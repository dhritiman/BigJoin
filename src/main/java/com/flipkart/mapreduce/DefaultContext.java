package com.flipkart.mapreduce;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by dhritiman.das on 4/24/16.
 */
public class DefaultContext<K,V> implements Context<K,V>{

    private int numMappers;
    private int numReducers;
    private Map<Integer,ConcurrentLinkedQueue<V>> reducerQueues; // input queues for reducers
    private final AtomicInteger emitted = new AtomicInteger(0); // number of emitted tuples
    private final AtomicInteger finishedMappers = new AtomicInteger(0); // number of finished mappers
    private final AtomicBoolean allMapFinished = new AtomicBoolean(false);

    /*
     *
     */
    public DefaultContext(int numMappers, int numReducers)
    {
        this.numMappers = numMappers;
        this.numReducers = numReducers;
        this.reducerQueues = new HashMap<Integer, ConcurrentLinkedQueue<V>>();
        init();
    }


    /* This method will be called by mapper at the end of map() method
     * We will push this result value V in the input queue of Reducer
     * for key
     */
    @Override
    public void emit(K key, V value) {

        emitted.incrementAndGet();
        ConcurrentLinkedQueue<V> reducerQueue = getOrCreateQueue(key);
        reducerQueue.add(value);

    }

    @Override
    public void mapFinished() {
        finishedMappers.incrementAndGet();

        //TODO take action if all mappers are done.
        if(finishedMappers.intValue() == numMappers)
        {
            allMapFinished.set(true);
        }
    }

    @Override
    public ConcurrentLinkedQueue getQueueForReducer(K key) {

        ConcurrentLinkedQueue<V> queue = null;
        if(reducerQueues.containsKey(key))
            queue = reducerQueues.get(key);

        return queue;
    }

    @Override
    public boolean allMapFinished() {
        //System.out.println("allMapFinished() called in reducer thread .. " + Thread.currentThread());
        return allMapFinished.get();
    }


    /* Private functions */
    private ConcurrentLinkedQueue<V> getOrCreateQueue(K key)
    {
        ConcurrentLinkedQueue<V> queue = null;
        if(!reducerQueues.containsKey(key))
        {
            queue = new ConcurrentLinkedQueue<V>();
        }
        else
        {
            queue = reducerQueues.get(key);
        }
        //reducerQueues.put((Integer)key,queue);
        return queue;
    }

    private void init()
    {
        for(int i = 0; i < numReducers; i++)
        {
            reducerQueues.put(i,new ConcurrentLinkedQueue<V>());
        }
    }


}
