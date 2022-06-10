/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.gridgain.demo.datamuse;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.asynchttpclient.BoundRequestBuilder;
import org.asynchttpclient.Dsl;
import org.asynchttpclient.Response;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.ExecutionException;

public class DataMuseClient {

    private static final String DATAMUSE_API_URL = "https://api.datamuse.com/words";
    private static BoundRequestBuilder request;

    private static final ObjectMapper MAPPER = new ObjectMapper();

    //TODO split request

    public static List<DataMuseWord> getWord(String topic) throws ExecutionException, InterruptedException, IOException {
        request = Dsl.asyncHttpClient().prepareGet(DATAMUSE_API_URL);
        request.addQueryParam("max", Integer.toString(Math.min(200, 1000))); // default to 500 for all our requests
        StringBuilder topicsParam = new StringBuilder();
        for (int i = 0; i < 5; i++) {
            topicsParam.append(topic);

            if(i+1 < 5)
                topicsParam.append(",");
        }
        request.addQueryParam("topics", topicsParam.toString());

        Response response = request.execute().get();
        System.out.println(response.getResponseBody());
        return MAPPER.readValue(response.getResponseBody(),
                new TypeReference<List<DataMuseWord>>() {});
    }

    public static List<DataMuseWord> getWordStartingWith(String s) throws ExecutionException, InterruptedException, IOException {
        request = Dsl.asyncHttpClient().prepareGet(DATAMUSE_API_URL);
        request.addQueryParam("max", Integer.toString(Math.min(200, 1000)));
        StringBuilder topicsParam = new StringBuilder();

        request.addQueryParam("sp", s + "*");

        Response response = request.execute().get();
        System.out.println(response.getResponseBody());
        return MAPPER.readValue(response.getResponseBody(),
                new TypeReference<List<DataMuseWord>>() {});
    }
}
