package com.flipkart.mrapps.wcdemo;

import com.flipkart.mapreduce.*;

import java.util.HashMap;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.StringTokenizer;

/**
 * Created by dhritiman.das on 4/25/16.
 */
public class WCDemoApp {


    public static void main(String [] args)
    {
        //KeyIn is a array index
        //ValueIn is a
        InputData data = new InputData();
        MapReduceEngine<Integer,Integer> wcDemoEngine = new MapReduceEngine<Integer, Integer>()
                                                                .mapperFactory(new WCMapperFactory(data))
                                                                .reducerFactory(new WCReducerFactory())
                                                                .mappers(3)
                                                                .reducers(data.getNumReducers())
                                                                .readerFactory(new BasicReaderFactory(data,3))
                                                                .init()
                                                                .submit();
    }

}

class InputData
{
    public String[] data = { "One Two Three",
                             "Two Three",
                             "One Four Five",
                             "One Three",
                             "Four Three",
                             "Two Four Five" };


    private Map<String,Integer> reducerMap;

    public int getSize()
    {
        return data.length;
    }

    public InputData()
    {
        reducerMap = new HashMap<String, Integer>();
        int reducer = 0;
        for(int i =0 ;i < data.length; i++)
        {
            StringTokenizer tokenizer = new StringTokenizer(((String)data[i]).toLowerCase());
            while (tokenizer.hasMoreTokens()) {
               String next = tokenizer.nextToken();
               if(!reducerMap.containsKey(next))
               {
                   System.out.println("Reducer Map - " + next + " --- " + reducer);
                   reducerMap.put(next,reducer++);
               }

            }
        }
    }

    public int getNumReducers()
    {
        return reducerMap.size();
    }

    public int getReducerForKey(String key)
    {
        return reducerMap.get(key);
    }


}

class WCMapperFactory implements MapperFactory
{
    private InputData data;

    public WCMapperFactory(InputData data)
    {
        this.data = data;
    }

    @Override
    public Mapper newMapper() {
        return new WCMapper(data);
    }
}

class WCMapper implements Mapper
{
    private InputData data;

    public WCMapper(InputData data)
    {
        this.data = data;
    }

    @Override
    public void map(Object key, Object value, Context context)
    {
        System.out.println("Mapper map() in Thread " + Thread.currentThread().getName());

        StringTokenizer tokenizer = new StringTokenizer(((String)value).toLowerCase());

        // For every token in the text (=> per word)
        while (tokenizer.hasMoreTokens()) {
            // Emit a new value in the mapped results
            //context.emit(tokenizer.nextToken(), 1);
            context.emit(data.getReducerForKey(tokenizer.nextToken()), 1);
        }

    }
}

class MappingKeyToReducerMapper<K1,K2>
{
    private Map<K1,K2> mappingToReducer;
    private static volatile MappingKeyToReducerMapper instance;
    private static final Object object = new Object();

    private MappingKeyToReducerMapper()
    {
        mappingToReducer = new HashMap<K1, K2>();
    }

    public MappingKeyToReducerMapper getInstance()
    {
        if(instance != null)
        {
            synchronized(object)
            {
                if(instance != null)
                {
                    instance = new MappingKeyToReducerMapper();
                }
            }
        }

        return instance;
    }

    public void add(K1 key, K2 value)
    {
        mappingToReducer.put(key,value);
    }

    public K2 get(K1 key)
    {
        return mappingToReducer.get(key);
    }

}

class WCReducerFactory implements ReducerFactory
{

    @Override
    public Reducer newReducer() {
        return new WCReducer();
    }
}

class WCReducer implements Reducer
{
    private int count ;

    @Override
    public void beginReduce(Object key) {

        System.out.println("beginReduce() for key " + key);
        count = 0;

    }

    @Override
    public void reduce(Object value) {

        count += (Integer)value;
        System.out.println("Called reduce with value =  " + value + " - count becomes " + count);

    }

    @Override
    public void finalizeReduce(Object key) {

        System.out.println("Reduced Value for Key " + key + " is " + count);
    }
}

class BasicReaderFactory implements ReaderFactory<KeyValue<Integer,String>>
{
    private InputData inputData;
    private int numPartitions;

    public BasicReaderFactory(InputData inputData, int numPartitions)
    {
        this.inputData = inputData;
        this.numPartitions = numPartitions;
    }

    @Override
    public PartitionAwareIterator<KeyValue<Integer,String>> getReaderForPartition(int partitionId) {
        int size = inputData.getSize();
        int partitionLength = size / numPartitions;
        int start = partitionId*partitionLength;
        int end = (start + partitionLength <= inputData.getSize())? start+partitionLength : inputData.getSize();
        return new RangeIterator(inputData, start,end);

    }
}

class RangeIterator implements PartitionAwareIterator<KeyValue<Integer,String>>
{
    private InputData inputData;
    private int start;
    private int end;
    private int cursor;

    public RangeIterator(InputData inputData, int start, int end)
    {
        this.inputData = inputData;
        this.start = start;
        this.end = end;
        this.cursor = start;
    }

    @Override
    public boolean hasNext() {
        return this.cursor < end;
    }

    @Override
    public KeyValue<Integer,String> next() {
        if(!this.hasNext())
            throw new NoSuchElementException();
        String value = inputData.data[cursor];
        return new KeyValue<Integer, String>(cursor++,value);
    }
}
