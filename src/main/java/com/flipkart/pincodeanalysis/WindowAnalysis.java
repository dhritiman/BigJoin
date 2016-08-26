package com.flipkart.pincodeanalysis;

import java.io.*;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by dhritiman.das on 8/8/16.
 */

class ZonePair{
    public String zone1;
    public String zone2;

    public ZonePair(String zone1, String zone2){
        this.zone1 = zone1;
        this.zone2 = zone2;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof ZonePair)) return false;

        ZonePair zonePair = (ZonePair) o;

        if (zone1 != null ? !zone1.equals(zonePair.zone1) : zonePair.zone1 != null) return false;
        if (zone2 != null ? !zone2.equals(zonePair.zone2) : zonePair.zone2 != null) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = zone1 != null ? zone1.hashCode() : 0;
        result = 31 * result + (zone2 != null ? zone2.hashCode() : 0);
        return result;
    }
}
public class WindowAnalysis {

    private static Map<ZonePair,Integer> zonePairs = new HashMap<>();
    private static Map<String, Integer> pincodeUniq = new HashMap<>();
    private static Map<String, Integer> zone1Uniq = new HashMap<>();
    private static Map<String, Integer> zone2Uniq = new HashMap<>();

    public static void main(String [] args){

        try{
            File in = new File(args[0]);
            File out = new File(args[1]);

            BufferedReader br = new BufferedReader(new FileReader(in));
            FileWriter writer = new FileWriter(out);

            String line = null;

            int i = 1;
            Date lastDate = null;
            while( (line = br.readLine()) != null){
                String [] arr = line.split(" ");
                String date = arr[0];
                SimpleDateFormat parserSDF=new SimpleDateFormat("HH:mm:ss.SSS");
                Date d = parserSDF.parse(date);
                if(lastDate == null){
                    lastDate = d;
                }
                String pincode = arr[1];
                String zone1 = arr[2];
                String zone2 = arr[3];

                if(pincodeUniq.containsKey(pincode)){
                    pincodeUniq.put(pincode, pincodeUniq.get(pincode) + 1);
                }
                else
                    pincodeUniq.put(pincode,1);

                ZonePair zp = new ZonePair(zone1, zone2);

                if(zonePairs.containsKey(zp)){
                    zonePairs.put(zp, zonePairs.get(zp) + 1);
                }else
                zonePairs.put(zp,1);

                if(zone1Uniq.containsKey(zone1)){
                    zone1Uniq.put(zone1, zone1Uniq.get(zone1) + 1);
                }
                else
                    zone1Uniq.put(zone1, 1);

                if(zone2Uniq.containsKey(zone2)){
                    zone2Uniq.put(zone2, zone2Uniq.get(zone2) + 1);
                }
                else
                    zone2Uniq.put(zone2, 1);


                if( ((d.getTime() - lastDate.getTime())/1000) > 900 ){
                    //System.out.println("START: " + lastDate + " END: " + d);
                    writer.write("[" + lastDate + "---" + d + "]" + " :: ");
                    printStats(writer);
                    flushStats();
                    lastDate = null;
                }
                i++;
            }
            writer.flush();
            writer.close();
        }catch (Exception ex){
            ex.printStackTrace(System.err);
        }

    }

    private static void flushStats() {
        System.out.println("Clearing..");
        pincodeUniq.clear();
        zonePairs.clear();
        zone1Uniq.clear();
        zone2Uniq.clear();
    }

    private static void printStats(FileWriter writer) throws IOException {

        //System.out.println("new window stats :-  Unique pincodes = " + pincodeUniq.size() + " - Unique zone pairs = " + zonePairs.size() );
        writer.write(pincodeUniq.size() + "  " + zonePairs.size() + " " + zone1Uniq.size() + " " + zone2Uniq.size());
        writer.write("\n");
    }
}
