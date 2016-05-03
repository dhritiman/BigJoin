package com.flipkart.mrapps.distributedjoin;

import com.flipkart.constants.Enums;
import com.flipkart.mapreduce.*;
import org.apache.lucene.util.OpenBitSet;

import java.util.ArrayList;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Random;

/**
 * Created by dhritiman.das on 5/2/16.
 */
public class DistributedJoinApp {

    public static void main(String [] args)
    {
        //KeyIn is a array index
        //ValueIn is a
        DistributedJoinInputData data = new DistributedJoinInputData();
        System.out.println("Start with filling data..");
        data.fillData();
        System.out.println("Done with filling data..");

        System.out.println("MRSTART: Start MR engine at time " + System.currentTimeMillis());
        MapReduceEngine<Integer,DJReducerValue> djengine = new MapReduceEngine<Integer, DJReducerValue>()
                .mapperFactory(new DJMapperFactory(data))
                .reducerFactory(new DJReducerFactory())
                .mappers(20)
                .reducers(data.getNumReducers())
                .readerFactory(new BasicReaderFactory(data,20))
                .init()
                .submit();

    }
}

class DistributedJoinInputData
{
    public int [][] listingSource;  // Listing is ordinal
    public int [][] listingAttribute; // Listing is ordinal
    public OpenBitSet[] shToPincodeServicability; // Source-Hash is ordinal

    //public static final int LISTINGS = 10000000;  //actual per shard 10M
    public static final int LISTINGS = 8000000;  //actual per shard 10M
    public static final int MAX_SOURCES =5;  //average per listing 3-4
    public static final int MAX_ATTRIBUTES = 5;  //average per listing 5
    public static final int MAX_PINCODES = 20000; // scale to 20000
    public static final int MAX_SOURCE_HASHES = 500000; // estimated - 500K

    public static final String hashSeprator = "_".intern();
    public static final String emptyString = "".intern();

    public DistributedJoinInputData()
    {
        listingSource = new int[LISTINGS][MAX_SOURCES]; // Assuming a maximum of MAX_SOURCES per listing - extras will be 0 or -1
        listingAttribute = new int[LISTINGS][MAX_ATTRIBUTES]; // Assuming a maximum of MAX_ATTRIBUTES per sources
        shToPincodeServicability = new OpenBitSet[MAX_SOURCE_HASHES];
    }

    public void fillData()
    {
        //Fill Ds1
        for(int i = 0 ; i < LISTINGS ; i++)
        {
            for(int j = 0 ; j < MAX_SOURCES; j++ )
            {
                listingSource[i][j] = Enums.getSource(random(1, 20));
            }
        }
        System.out.println("Done with Listing-Source");

        //Fill Ds2
        //Consider attribute values possible from
        for(int i = 0 ; i < LISTINGS ; i++)
        {
            listingAttribute[i][0] = random(1, 10);
            listingAttribute[i][1] = random(12, 15);
            listingAttribute[i][2] = random(16, 22);
            listingAttribute[i][3] = random(23, 30);
            listingAttribute[i][4] = random(31, 34);
        }

        System.out.println("Done with Listing-Attributes");

        //Fill Ds3
        //Consider MAX_SOURCE_HASHES source-hashes and initialize each source hash with a open bit set
        //and change the
        for(int i = 0 ; i < MAX_SOURCE_HASHES; i++)
        {
            OpenBitSet bitSet = new OpenBitSet(MAX_PINCODES);
            //TODO this part is commented as it doesn't affect join time
            //else taking too much time to load this stuff
            /*
            for(int b = 0 ; b < MAX_PINCODES; b++ )
            {
                //toss a coin
                if(random(0,2) == 1)
                {
                    bitSet.set(b);
                }
            }
            */
            shToPincodeServicability[i] = bitSet;
        }

        System.out.println("Data Preparation complete");

    }

    public int random(int x, int y)
    {
        Random r = new Random();
        int low = x;
        int high = y;
        int result = r.nextInt(high-low) + low;
        return result;
    }

    //Accessor methods for helping mappers and reducers
    private static final int numReducers = 24; //Elec + Books + Lifestyle ( 8 shards each - each split into 3 parts ) 24*3=72

    public int getNumReducers()
    {
        return numReducers;
    }

    public int getSize()
    {
        return LISTINGS;
    }
}

class DJMapperFactory implements MapperFactory
{
    private DistributedJoinInputData distributedJoinInputData;

    public DJMapperFactory(DistributedJoinInputData distributedJoinInputData)
    {
        this.distributedJoinInputData = distributedJoinInputData;
    }
    @Override
    public Mapper newMapper() {
        return new DJMapper(distributedJoinInputData);
    }
}

class DJMapper implements Mapper
{
    private DistributedJoinInputData distributedJoinInputData;

    public DJMapper(DistributedJoinInputData distributedJoinInputData)
    {
        this.distributedJoinInputData = distributedJoinInputData;
    }

    @Override
    public void map(Object key, Object value, Context context)
    {
        //System.out.println("Mapper map() in Thread " + Thread.currentThread().getName() + " Key= " + (Integer)key + " Value= " + (int[])value);

        //Key - Listing Ord
        //Value - int [] : source ord array for which this listing is available

        //TODO Join
        OpenBitSet listingServicability = new OpenBitSet(distributedJoinInputData.MAX_PINCODES);
        int listingOrd = (Integer)key;
        int[] listingSourceOrds = (int[])value;
        int numSourcesInThisListing = listingSourceOrds.length;
        //For each source construct the source hash
        for(int j = 0; j < numSourcesInThisListing ; j++ )
        {
                //TODO ignore this source if the value if 0 or -1 or whatever we decide

                //Generate source hash
                //TODO Use shared library to compute the hash
                //TODO this string manipulation is creating a GC overhead limit exceeded error
                //Because of string manipulations. Needs to be kept in mind for the library call
                //TODO or create a separate structure with Listing to SourceHash before hand first
                // for now commenting this part as we are anyways taking a ORD

                String sourceHash = listingSourceOrds[j] + distributedJoinInputData.emptyString ;
                for(int k = 0; k < distributedJoinInputData.MAX_ATTRIBUTES ; k++)
                {
                    sourceHash += distributedJoinInputData.hashSeprator + distributedJoinInputData.listingAttribute[k];
                }

                //TODO lookup sourceHashToOrd map to get sourcehash ordinal
                //This source-hash will be represented by a ordinal which will be already
                //present in source-hash to ordinal map.
                //Simulate that for now with a random number lookup in the same range
                int sourceHashOrd = distributedJoinInputData.random(0, distributedJoinInputData.MAX_SOURCE_HASHES);

                //Get bitset for this sourceHash
                //TODO here we need to get the bitset from the appropriate pincode split range vertically
                //using entire range for now
                OpenBitSet bitSet = distributedJoinInputData.shToPincodeServicability[sourceHashOrd];
                //System.out.println("Doing a union with sourceHashOrd " + sourceHashOrd + " for listing " + i);
                listingServicability.union(bitSet);
            }

        //Emit to context
        //Key - Integer representing the reducer/shard for this Listing
        //Value - <ListingOrd,OpenBitSet>
        DJReducerValue reducerValue = new DJReducerValue(listingOrd,listingServicability);
        context.emit(listingOrd%(distributedJoinInputData.getNumReducers()),reducerValue);
    }
}

class DJReducerValue
{
    public int listingOrd;
    public OpenBitSet computedBitset;

    public DJReducerValue(int listingOrd, OpenBitSet computedBitset)
    {
        this.listingOrd = listingOrd;
        this.computedBitset = computedBitset;
    }
}

class DJReducerFactory implements ReducerFactory
{
    @Override
    public Reducer newReducer() {
        return new DJReducer();
    }
}

class DJReducer implements Reducer
{
    private List<DJReducerValue> shardData = new ArrayList<DJReducerValue>();

    @Override
    public void beginReduce(Object key) {
        System.out.println("beginReduce() for key " + key);
    }

    @Override
    public void reduce(Object value) {
        //System.out.println("reduce() called with value =  " + value + " in reducer " + Thread.currentThread() );
        shardData.add((DJReducerValue)value);
    }

    @Override
    public void finalizeReduce(Object key) {

        System.out.println("Finalize Reduce for Key  " + key + " --> Total rows to dump = " + shardData.size());
        //Dump shardData
    }
}

class BasicReaderFactory implements ReaderFactory<KeyValue<Integer,int[]>>
{
    private DistributedJoinInputData inputData;
    private int numPartitions;

    public BasicReaderFactory(DistributedJoinInputData inputData, int numPartitions)
    {
        this.inputData = inputData;
        this.numPartitions = numPartitions;
    }

    @Override
    public PartitionAwareIterator<KeyValue<Integer,int[]>> getReaderForPartition(int partitionId) {
        int size = inputData.getSize();
        int partitionLength = size / numPartitions;
        int start = partitionId*partitionLength;
        int end = (start + partitionLength <= inputData.getSize())? start+partitionLength : inputData.getSize();
        return new RangeIterator(inputData, start,end);

    }
}

class RangeIterator implements PartitionAwareIterator<KeyValue<Integer,int[]>>
{
    private DistributedJoinInputData inputData;
    private int start;
    private int end;
    private int cursor;

    public RangeIterator(DistributedJoinInputData inputData, int start, int end)
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
    public KeyValue<Integer,int[]> next() {
        if(!this.hasNext())
            throw new NoSuchElementException();
        int[] value = inputData.listingSource[cursor];
        return new KeyValue<Integer, int[]>(cursor++,value);
    }
}
