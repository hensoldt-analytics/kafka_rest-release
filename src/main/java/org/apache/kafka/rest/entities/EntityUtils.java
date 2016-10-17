/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 **/

package org.apache.kafka.rest.entities;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import javax.xml.bind.DatatypeConverter;
import java.io.IOException;

public class EntityUtils {
    private static ObjectMapper mapper = new ObjectMapper();

    public static byte[] parseBase64Binary(String data) throws IllegalArgumentException {
        try {
            return DatatypeConverter.parseBase64Binary(data);
        } catch (ArrayIndexOutOfBoundsException e) {
            // Implementation can throw index error on invalid inputs, make sure all known parsing issues
            // get converted to illegal argument error
            throw new IllegalArgumentException(e);
        }
    }

    public static String encodeBase64Binary(byte[] data) {
        return DatatypeConverter.printBase64Binary(data);
    }

    public static byte[] jsonSerialize(Object jsonObject) {
        if(jsonObject == null)
            return null;

        try {
            return mapper.writeValueAsBytes(jsonObject);
        } catch (JsonProcessingException e) {
            throw new IllegalArgumentException("Exception while serializing json object");
        }
    }

    public static Object jsonDeSerialize(byte[] jsonBytes) {
        if(jsonBytes == null)
            return null;

        try {
            return mapper.readValue(jsonBytes, Object.class);
        } catch (IOException e) {
            throw new IllegalArgumentException("Exception while de-serializing json object");
        }
    }

    public static void main(String[] args) {
        //System.out.println(parseBase64Binary("aGVsbG8=="));
    }
}
