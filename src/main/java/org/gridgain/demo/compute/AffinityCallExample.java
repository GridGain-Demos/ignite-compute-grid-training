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

package org.gridgain.demo.compute;

import org.apache.ignite.*;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.affinity.Affinity;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.cache.query.ScanQuery;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.lang.IgniteBiPredicate;
import org.apache.ignite.lang.IgniteCallable;
import org.apache.ignite.lang.IgniteRunnable;
import org.apache.ignite.resources.IgniteInstanceResource;
import org.apache.ignite.resources.LoggerResource;
import org.gridgain.demo.datamuse.DataMuseClient;
import org.gridgain.demo.datamuse.DataMuseWord;
import org.gridgain.demo.model.WordKey;

import javax.cache.Cache;
import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ExecutionException;

/**
 * This demo shows that if we are running compute job by key affinity using the method
 * {@link org.apache.ignite.IgniteCompute#affinityRun(String, Object, IgniteRunnable)}
 * then we will actually run the task exactly on the node where primary partition for the given key
 * is located.
 * <p>
 * The node that will run this task or closure could be predicted using the cache affinity
 * which could be obtained by {@link Affinity#mapKeyToNode(Object)}.
 * <p>
 * Running distributed computations on affinity nodes could also be used for execution
 * of operations on the whole data set, such as complex overnight calculations and the tasks will eventually
 * get distributed in the same way as data partitions are distributed across the cluster nodes.
 */
public class AffinityCallExample extends ComputeDemo {

    private final static String letters = "abcdefghijklmnopqrstuvwxyz";

    private final static String INT_CACHE_NAME = "IntCache";

    public static void main(String[] args) throws IOException, ExecutionException, InterruptedException {
        Ignition.setClientMode(true);
        Ignite ignite = Ignition.start(CONFIG_PATH + "/ignite-config-node.xml");

        ClusterState clusterState = ignite.cluster().state();
        if (!clusterState.equals(ClusterState.ACTIVE))
            ignite.cluster().state(ClusterState.ACTIVE);

        ignite.getOrCreateCache("WordCache");

        Affinity<String> affinity = ignite.affinity("WordCache");

        // Find primary partition owner for letter 'a' (node A).
        ClusterNode nodeA = affinity.mapKeyToNode(letters.substring(0, 1));
        ClusterNode nodeB = null;

        String letterA = "a";
        String mysteryLetter = null;

        // Find primary partition owner for another letter that differs from node A.
        for (int i = 1; i < letters.length(); i++) {
            String s = letters.substring(i, i + 1);

            ClusterNode candidate = affinity.mapKeyToNode(s);
            if (candidate.id().equals(nodeA.id()))
                continue;

            mysteryLetter = s;
            nodeB = candidate;

            break;
        }

        // Load words into the cache.
        List<DataMuseWord> wordStartingWithA = ignite.compute().call(new LoadWordsCallable(letterA, "Don't be that suspicious."));
        List<DataMuseWord> wordStartingWithX = ignite.compute().call(new LoadWordsCallable(mysteryLetter, "You never know what's best."));

        // Submit affinityRun to print the words on expected node.
        for (DataMuseWord w : wordStartingWithA)
            ignite.compute().affinityRun("WordCache",
                    w.getWord().substring(0, 1),
                    new FirstLetterAffinityRun(w.getWord(), nodeA.consistentId()));

        for (DataMuseWord w : wordStartingWithX)
            ignite.compute().affinityRun("WordCache",
                    w.getWord().substring(0, 1),
                    new FirstLetterAffinityRun(w.getWord(), nodeB.consistentId()));

        IgniteCache<Integer, Integer> simpleCounterCache = ignite.getOrCreateCache(INT_CACHE_NAME);

        for (int i = 0; i < ignite.affinity(INT_CACHE_NAME).partitions(); i++)
            simpleCounterCache.put(i, i);

        runPerPartitionComputeByAffinity(ignite, INT_CACHE_NAME);

        runPerPartitionComputeByBroadcast(ignite, INT_CACHE_NAME);

        ignite.close();
    }

    public static void runPerPartitionComputeByBroadcast(Ignite ignite, String cacheName) {
        ignite.compute().broadcast(new PerPartitionBroadcastCompute(cacheName));
    }

    @SuppressWarnings("rawtypes")
    public static class PerPartitionBroadcastCompute implements IgniteRunnable {

        @IgniteInstanceResource
        Ignite ignite;

        private final String cacheName;

        public PerPartitionBroadcastCompute(String cacheName) {
            this.cacheName = cacheName;
        }

        @Override
        public void run() {
            System.out.println(">>> Started running " + this.getClass().getCanonicalName() + " compute job/task.");

            ClusterNode localNode = ignite.cluster().localNode();

            IgniteCache<Integer, Integer> simpleCounterCache = ignite.getOrCreateCache(cacheName);

            int[] localPrimaryPartitions = ignite.affinity(cacheName).primaryPartitions(localNode);

            for (int partition : localPrimaryPartitions) {
                ScanQuery query = new ScanQuery((IgniteBiPredicate)(x1, x2) -> true);
                query.setPartition(partition);

                Iterator<Cache.Entry<Integer, Integer>> it = simpleCounterCache.query(query).iterator();

                while (it.hasNext()) {
                    Cache.Entry<Integer, Integer> entry = it.next();

                    Integer key = entry.getKey();
                    Integer val = entry.getValue();

                    System.out.println("Key = " + key + ", val = " + val + ".");
                }
            }

            System.out.println(">>> Finished running " + this.getClass().getCanonicalName() + " compute job/task.");
        }
    }

    public static void runPerPartitionComputeByAffinity(Ignite ignite, String cacheName) {
        int partitions = ignite.affinity(cacheName).partitions();

        for (int i = 0; i < partitions; i++) {
            ignite.compute().affinityRun(cacheName, i, new PerPartitionAffinityCompute(cacheName, i));
        }
    }

    @SuppressWarnings("rawtypes")
    public static class PerPartitionAffinityCompute implements IgniteRunnable {

        @IgniteInstanceResource
        Ignite ignite;

        private final String cacheName;

        private final int partitionId;

        public PerPartitionAffinityCompute(String cacheName, int partitionId) {
            this.cacheName = cacheName;
            this.partitionId = partitionId;
        }

        @Override
        public void run() {
            System.out.println(">>> Started running " + this.getClass().getCanonicalName() + " compute job/task.");

            IgniteCache<Integer, Integer> simpleCounterCache = ignite.getOrCreateCache(cacheName);

            ScanQuery query = new ScanQuery((IgniteBiPredicate)(x1, x2) -> true);
            query.setPartition(partitionId);

            Iterator<Cache.Entry<Integer, Integer>> it = simpleCounterCache.query(query).iterator();

            while (it.hasNext()) {
                Cache.Entry<Integer, Integer> entry = it.next();

                Integer key = entry.getKey();
                Integer val = entry.getValue();

                System.out.println("Key = " + key + ", val = " + val + ".");
            }

            System.out.println(">>> Finished running " + this.getClass().getCanonicalName() + " compute job/task.");
        }
    }

    public static class LoadWordsCallable implements IgniteCallable<List<DataMuseWord>> {

        private String letter;

        private String somePhrase;

        @IgniteInstanceResource
        Ignite ignite;

        public LoadWordsCallable(String letter, String somePhrase) {
            this.letter = letter;
            this.somePhrase = somePhrase;
        }

        @Override
        public List<DataMuseWord> call() throws Exception {
            System.out.println(">>> Started running " + this.getClass().getCanonicalName() + " compute job/task.");

            CacheConfiguration<WordKey, String> ccfg = new CacheConfiguration<>();
            ccfg.setAffinity(new RendezvousAffinityFunction(false));
            ccfg.setCacheMode(CacheMode.PARTITIONED);
            ccfg.setBackups(1);

            IgniteCache<WordKey, String> wordCache = ignite.getOrCreateCache("WordCache");

            List<DataMuseWord> wordStartingWith = DataMuseClient.getWordStartingWith(letter);

            for (DataMuseWord w : wordStartingWith)
                wordCache.put(new WordKey(w.getWord(), letter), somePhrase);

            System.out.println(">>> Finished running " + this.getClass().getCanonicalName() + " compute job/task.");

            return wordStartingWith;
        }
    }

    public static class FirstLetterAffinityRun implements IgniteRunnable {
        @LoggerResource
        IgniteLogger logger;

        @IgniteInstanceResource
        Ignite ignite;

        private String word;
        private Object id;

        public FirstLetterAffinityRun(String word, Object id) {
            this.word = word;
            this.id = id;
        }

        @Override
        public void run() {
            System.out.println(">>> Started running " + this.getClass().getCanonicalName() + " compute job/task.");

            String r = String.valueOf(ignite.cache("WordCache").get(new WordKey(word, word.substring(0, 1))));

            log(logger, "Hello there! The job was initially sent to cluster node : " + id);
            log(logger, "I am a node : " + ignite.cluster().localNode().consistentId());
            log(logger, "The word is : " + word);
            log(logger, "The text you get is : " + r);

            System.out.println(">>> Finished running " + this.getClass().getCanonicalName() + " compute job/task.");
        }
    }
}
