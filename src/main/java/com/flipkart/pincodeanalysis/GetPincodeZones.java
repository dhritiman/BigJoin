package com.flipkart.pincodeanalysis;

import org.springframework.http.*;
import org.springframework.web.client.RestTemplate;

import java.io.*;

/**
 * Created by dhritiman.das on 8/8/16.
 */
public class GetPincodeZones {

    private static RestTemplate restTemplate = new RestTemplate();

    static String getZone(String pincode){

        try{
        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);
        headers.set("X-Flipkart-Client","w3.sherlock");
        headers.set("X-Request-Id","r1");

        HttpEntity<String> entity = new HttpEntity<String>("", headers);
        ResponseEntity<String> responseEntity = restTemplate.exchange("http://10.47.1.31/zone/pincode/"+pincode, HttpMethod.GET, entity, String.class);
        return responseEntity.getBody();
        }catch (Exception ex){
            return "CALLFAILED";
        }finally {

        }
    }

    static String getSZone(String pincode){
        try{
        String szone = restTemplate.getForObject("http://10.47.2.101/v1/pincodes/"+pincode, String.class);
        return szone;
        }catch (Exception ex){
            return "CALLFAILED";
        }finally {

        }
    }

    public static void main(String [] args) {

        try{
        File in = new File(args[0]);
        File out = new File(args[1]);

        BufferedReader br = new BufferedReader(new FileReader(in));
        FileWriter writer = new FileWriter(out);

        String line = null;

        int i = 1;
        while( (line = br.readLine()) != null){
            String pincodeString = line.split(" ")[1];
            String pincode =pincodeString.split("=")[1];
            String zone = getZone(pincode);
            String szone = getSZone(pincode);
            String newline = line + " " + zone + " " + szone;
            writer.write(newline);
            writer.write("\n");
            //System.out.println(newline);
            i++;
            if((i % 100 ) == 0){
                System.out.println("Donw with " + i + " pincodes.");
                writer.flush();
            }


        }
        writer.flush();
        writer.close();
        }catch (Exception ex){
            ex.printStackTrace(System.err);
        }

    }
}
