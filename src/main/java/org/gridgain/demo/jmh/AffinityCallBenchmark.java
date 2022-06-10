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

package org.gridgain.demo.jmh;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.affinity.Affinity;
import org.apache.ignite.cluster.ClusterGroup;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.lang.IgniteRunnable;
import org.apache.ignite.resources.IgniteInstanceResource;
import org.apache.ignite.resources.LoggerResource;
import org.gridgain.demo.datamuse.DataMuseClient;
import org.gridgain.demo.datamuse.DataMuseWord;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@State(Scope.Benchmark)
public class AffinityCallBenchmark {

    private static final int N = 100_000;

    private static Ignite clientNode;
    private static List<String> testKeys;
    private static final String name = "AddressBook";
    private static ClusterNode affinityNode;
    private static ClusterGroup computeGroup;

    public static void main(String[] args) throws RunnerException {
        Options opt = new OptionsBuilder()
                .include(AffinityCallBenchmark.class.getSimpleName())
                .forks(1)
                .threads(1)
                .warmupIterations(5)
                .measurementIterations(10)
                .build();

        new Runner(opt).run();

        System.exit(0);
    }

    @Setup
    public void setup() throws IOException, ExecutionException, InterruptedException {
        Ignition.setClientMode(true);
        clientNode = Ignition.start("config/ignite-config-node.xml");

        IgniteCache<String, String> addressBook = clientNode.getOrCreateCache(name);

        List<DataMuseWord> names = DataMuseClient.getWord("Name");
        List<DataMuseWord> cities = DataMuseClient.getWord("City");

        ClusterGroup srvClusterGroup = clientNode.cluster().forServers();
        Collection<ClusterNode> srvNodes = srvClusterGroup.nodes();

        Iterator<ClusterNode> clusterNodeIterator = srvNodes.iterator();

        affinityNode = clusterNodeIterator.next();
        ClusterNode anotherNode = clusterNodeIterator.next();

        Affinity<String> affinity = clientNode.affinity(name);
        List<String> affinityKeys = new ArrayList<>();

        for(int i = 0; i < 500; i++) {
            int a = ThreadLocalRandom.current().nextInt(names.size());
            int b = ThreadLocalRandom.current().nextInt(cities.size());

            String key = cities.get(b).getWord();
            addressBook.put(key, names.get(a).getWord());

            if (affinity.mapKeyToNode(key).id().equals(affinityNode.id()))
            {
                affinityKeys.add(key);
            }
        }

        computeGroup = clientNode.cluster().forNode(anotherNode);

        testKeys = affinityKeys;
    }

    @Benchmark
    public void affinityCall() {
        for(String key : testKeys)
            clientNode.compute().affinityRun(name, key, new ValueLogOutputRunnable(key));
    }

    @Benchmark
    public void runOnSpecificNode() {
        for(String key : testKeys)
            clientNode.compute(computeGroup).run(new ValueLogOutputRunnable(key));
    }

    @TearDown
    public void tearDown() {
        clientNode.close();
    }

    public static class ValueLogOutputRunnable implements IgniteRunnable {
        @LoggerResource
        IgniteLogger logger;

        @IgniteInstanceResource
        Ignite ignite;

        private final String t;

        public ValueLogOutputRunnable(String t) {
            this.t = t;
        }

        @Override
        public void run() {
            System.out.println(ignite.cache(name).get(t).toString());
            logger.info(ignite.cache(name).get(t).toString());
        }
    }
}
