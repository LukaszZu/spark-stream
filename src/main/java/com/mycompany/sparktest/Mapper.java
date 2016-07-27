/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.mycompany.sparktest;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.PropertyNamingStrategy;

/**
 *
 * @author zulk
 */
public class Mapper {

    private static class Holder {
        static final ObjectMapper INSTANCE;
        static {
            INSTANCE = new ObjectMapper();
            System.out.println("IIIIIIIII"+INSTANCE);
            INSTANCE.setPropertyNamingStrategy(PropertyNamingStrategy.LOWER_CASE);
            INSTANCE.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        }
    }
    
    public static ObjectMapper of() {
        return Holder.INSTANCE;
    }
    
}
