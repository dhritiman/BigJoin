package com.flipkart.avroserialization.uniontest;

import com.flipkart.avroserialization.forwardformat.FlatBitSet;
import com.google.common.base.Stopwatch;
import com.google.common.primitives.Longs;
import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileStream;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.util.Utf8;
import org.apache.lucene.util.OpenBitSet;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;

/**
 * Created by dhritiman.das on 6/4/16.
 */

class ZoneInfo
{
    public static final int MAX_ZONES = 8000;
}

class ListingInfo
{
    public static final int MAX_LISTINGS = 10000000;
}

class ForwardIndexedStore
{
    private FlatBitSet listingData;

    public ForwardIndexedStore()
    {
        listingData = new FlatBitSet(ListingInfo.MAX_LISTINGS, ZoneInfo.MAX_ZONES);
    }

    public void setListingBitSet(int listingOrd, long[] bitSetArray)
    {
        //TODO do some assertion checks
        listingData.updateOrdData(listingOrd, bitSetArray);
    }

}

class UnionSchema
{
    private Schema schema;
    private File file;
    private OpenBitSet bitset;
    private ForwardIndexedStore forwardIndexedStore;

    public UnionSchema(String file) throws IOException
    {
        ClassLoader classLoader = getClass().getClassLoader();
        schema = new Schema.Parser().parse(classLoader.getResourceAsStream("avro/unionschema.avsc"));
        this.file = new File(file);
        this.bitset = randomBitset();
        this.forwardIndexedStore = new ForwardIndexedStore();
    }

    private OpenBitSet randomBitset()
    {
        OpenBitSet s = new OpenBitSet(ZoneInfo.MAX_ZONES); // L -> zones
        Random r = new Random();
        for(long i = 0; i < ZoneInfo.MAX_ZONES ; i++)
        {
            float chance = r.nextFloat();
            if(chance <= 0.5f)
            {
                s.set(i);
            }
        }
        return s;
    }

    public void writeFile() throws IOException {

        Stopwatch stopwatch = new Stopwatch();
        stopwatch.start();

        //Common initialization
        File avroFile = file;
        GenericDatumWriter<Object> writer = new GenericDatumWriter<Object>(schema);

        ByteArrayOutputStream out = new ByteArrayOutputStream();
        BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(out, null);

        DatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<GenericRecord>(schema);
        DataFileWriter<GenericRecord> dataFileWriter = new DataFileWriter<GenericRecord>(datumWriter);

        dataFileWriter.create(schema, avroFile);

        //Write numZones
        GenericRecord avroNumRecords = new GenericData.Record(schema.getTypes().get(0));
        avroNumRecords.put("numZones",ZoneInfo.MAX_ZONES);
        writer.write(avroNumRecords,encoder);
        dataFileWriter.append(avroNumRecords);

        //Write header
        GenericRecord avroHeaderRecord = new GenericData.Record(schema.getTypes().get(1));
        for(int i = 0 ; i < ZoneInfo.MAX_ZONES; i++ )
        {
            avroHeaderRecord.put("zone","BOM"); //this will vary in real
            avroHeaderRecord.put("ordinal", i);
            writer.write(avroHeaderRecord,encoder);
            dataFileWriter.append(avroHeaderRecord);
        }

        dataFileWriter.close();

        //Write data
        dataFileWriter.appendTo(avroFile);
        GenericRecord avroDataRecord = new GenericData.Record(schema.getTypes().get(2));
        for(int i = 0 ; i < ListingInfo.MAX_LISTINGS; i++)
        {
            avroDataRecord.put("listing","LSTFCTDWZ2XWNCWSRDGAS7B51");
            avroDataRecord.put("bitsetLongArray", Longs.asList(bitset.getBits()));
            writer.write(avroDataRecord,encoder);
            dataFileWriter.append(avroDataRecord);
            if((i % 1000000) == 0)
            {
                System.out.println("Flushing after " + i + " rows");
                dataFileWriter.close();

                out = new ByteArrayOutputStream();
                encoder = EncoderFactory.get().binaryEncoder(out, null); // reinit the encoder
                //dataFileWriter = new DataFileWriter<GenericRecord>(datumWriter);
                dataFileWriter.appendTo(avroFile); // creates a new instance by appending to existing file
            }
        }
        dataFileWriter.close();
        stopwatch.stop();
        System.out.println("AVRO total time to to serialize " + stopwatch.elapsed(TimeUnit.MILLISECONDS));

    }

    public void readFile() throws IOException {

        Stopwatch stopwatch = new Stopwatch();
        stopwatch.start();

        File inputRecords = file;
        DatumReader<GenericRecord> datumReader = new GenericDatumReader<GenericRecord>(schema);
        DataFileReader<GenericRecord> dataFileReader = new DataFileReader<GenericRecord>(inputRecords, datumReader);

        //Read numZones
        GenericRecord numZonesRec = new GenericData.Record(schema.getTypes().get(0));
        int headerRecords = 0;
        if(dataFileReader.hasNext())
        {
            numZonesRec = dataFileReader.next(numZonesRec);
            headerRecords = (Integer)numZonesRec.get("numZones");
            System.out.println("numZones =" + headerRecords);
        }

        GenericRecord header = new GenericData.Record(schema.getTypes().get(1));
        //Read header
        for(int i = 0; i< headerRecords; i++)
        {
            header = dataFileReader.next(header);
            String zone = ((Utf8)header.get("zone")).toString();
            int ordinal = (Integer)header.get("ordinal");
            //System.out.println("zone : " + zone + " Ordinal: " + ordinal);
        }

        //Read data
        GenericRecord dataRecord = new GenericData.Record(schema.getTypes().get(2));
        int totalMessages = 0;
        while(dataFileReader.hasNext() && totalMessages < ListingInfo.MAX_LISTINGS)
        {
            dataRecord = dataFileReader.next(dataRecord);
            applyListingServiceabilityForward(dataRecord);
            totalMessages++;
            if((totalMessages % 1000000) == 0) System.out.println("Done with " + totalMessages + " rows ");
        }

        dataFileReader.close();

        stopwatch.stop();
        System.out.println("AVRO total time to to DE-serialize " + stopwatch.elapsed(TimeUnit.MILLISECONDS));
    }

    public void readFileFromStream(InputStream inputStream) throws IOException {

        Stopwatch stopwatch = new Stopwatch();
        stopwatch.start();

        DatumReader<GenericRecord> datumReader = new GenericDatumReader<GenericRecord>(schema);
        DataFileStream<GenericRecord> dataFileStream = new DataFileStream<GenericRecord>(inputStream, datumReader);

        //Read numZones
        GenericRecord numZonesRec = new GenericData.Record(schema.getTypes().get(0));
        int headerRecords = 0;
        if(dataFileStream.hasNext())
        {
            numZonesRec = dataFileStream.next(numZonesRec);
            headerRecords = (Integer)numZonesRec.get("numZones");
            System.out.println("numZones =" + headerRecords);
        }

        GenericRecord header = new GenericData.Record(schema.getTypes().get(1));
        //Read header
        for(int i = 0; i< headerRecords; i++)
        {
            header = dataFileStream.next(header);
            String zone = ((Utf8)header.get("zone")).toString();
            int ordinal = (Integer)header.get("ordinal");
            //System.out.println("zone : " + zone + " Ordinal: " + ordinal);
        }

        //Read data
        GenericRecord dataRecord = new GenericData.Record(schema.getTypes().get(2));
        int totalMessages = 0;
        while(dataFileStream.hasNext() && totalMessages < ListingInfo.MAX_LISTINGS)
        {
            dataRecord = dataFileStream.next(dataRecord);
            applyListingServiceabilityForward(dataRecord);
            totalMessages++;
            if((totalMessages % 1000000) == 0) System.out.println("Done with " + totalMessages + " rows ");
        }

        dataFileStream.close();

        stopwatch.stop();
        System.out.println("AVRO total time to to DE-serialize " + stopwatch.elapsed(TimeUnit.MILLISECONDS));
    }

    private void applyListingServiceabilityForward(GenericRecord listingRecord)
    {
        String listing = ((Utf8)listingRecord.get("listing")).toString();
        long[] bits = Longs.toArray((List<Long>)listingRecord.get("bitsetLongArray"));
        //System.out.println("Listing: " +listing + " - bits: " + bits.toString());

        //TODO get the listing ord given the listing id - store.getOrd(listing)
        //TODO do that bit outside this method in the caller
        //For now this is just a counter
        int ord = random(1, ListingInfo.MAX_LISTINGS);
        forwardIndexedStore.setListingBitSet(ord, bits);
    }

    private int random(int x, int y)
    {
        Random r = new Random();
        int low = x;
        int high = y;
        int result = r.nextInt(high - low) + low;
        return result;
    }
}

public class UnionSchemaTest {

    private static final String sandboxHost = "10.47.2.3";
    private static final String sandboxAccessKey = "52V64UOGK75EX6FWFS2J";
    private static final String sandboxSecretKey = "tR46e1QCDvES7pbYbAp34hAvzom3kO0D1jBNWUDP";

    public static void main( String[] args) throws IOException
    {
        //String file = "/Users/dhritiman.das/codebase/search/local-testing/union.avro";
        String file = "/var/lib/fk-w3-sherlock/avrotest/union.avro";
        UnionSchema unionSchema = new UnionSchema(file);

        /* For disk write and read
        //unionSchema.writeFile();
        //unionSchema.readFile();
        */

        D42Client client = new D42Client(sandboxHost,sandboxAccessKey,sandboxSecretKey);
        //uploadToD42(client,file,"ddtest1","unionFile");
        streamFromD42(client,"ddtest1","unionFile",unionSchema);
    }

    private static  void uploadToD42(D42Client client, String file, String bucketName, String key)
    {
        Stopwatch stopwatch = new Stopwatch();
        stopwatch.start();

        System.out.println("About to upload to D42..");
        client.put(bucketName,key, file);
        System.out.println("Done");

        stopwatch.stop();
        System.out.println("D42 Upload : " + stopwatch.elapsed(TimeUnit.MILLISECONDS));
    }

    private static  void streamFromD42(D42Client client, String bucketName, String key, UnionSchema unionSchema) throws IOException {
        Stopwatch stopwatch = new Stopwatch();
        stopwatch.start();
        System.out.println("About to download stream from D42..");
        InputStream stream = client.get(bucketName,key,512 * 1024 * 1024);
        stopwatch.stop();
        System.out.println("D42 Download : " + stopwatch.elapsed(TimeUnit.MILLISECONDS));

        System.out.println("Stream read starting..");
        unionSchema.readFileFromStream(stream);
        System.out.println("Done");

    }
}
