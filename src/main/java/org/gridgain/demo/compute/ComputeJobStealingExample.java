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

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCompute;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.Ignition;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.lang.IgniteClosure;
import org.apache.ignite.lang.IgniteFuture;
import org.apache.ignite.lang.IgniteReducer;
import org.apache.ignite.lang.IgniteRunnable;
import org.apache.ignite.resources.IgniteInstanceResource;
import org.apache.ignite.resources.LoggerResource;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * This demo shows how Job Stealing concept could be used to redistribute the compute jobs
 * between the cluster nodes and work like a late load balancing.
 * <p>
 * This example expects that the cluster uses default round-robin load balancing and we submit a number of
 * {@link ComputeJobStealingExample.EndlessRunnable} jobs that is (N = number of server nodes) less than total number
 * of threads on all N server nodes combined (calculated by {@link ComputeJobStealingExample.FindOutThreadPoolSize}.
 * <p>
 * After the jobs will be submitted for the execution, we will have exactly 1 free thread on each node which will
 * process all the {@link ComputeJobStealingExample.SwiftRunnable} that will be submitted onto the cluster.
 * <p>
 * We are enforcing jobs with even id to take 5 times more time that jobs with odd ids, and if the jobs were executed
 * on the nodes where they did initially arrive, the total execution will cost 100 seconds. With the job stealing,
 * instead of running 100 jobs each, one of the nodes will predictably run ~140 compute jobs, while the other will
 * run ~60 compute jobs and the total execution will cost 60 seconds.
 */
public class ComputeJobStealingExample extends ComputeDemo {

    public static void main(String[] args) {
        Ignition.setClientMode(true);
        Ignite ignite = Ignition.start(CONFIG_PATH + "/ignite-config-node.xml");

        ClusterState clusterState = ignite.cluster().state();

        if (!clusterState.equals(ClusterState.ACTIVE))
            ignite.cluster().state(ClusterState.ACTIVE);

        IgniteCompute compute = ignite.compute(ignite.cluster().forServers());

        // Calculate total number of threads capable of processing compute task across all server nodes.
        Integer totalPubThreadPoolSize = compute.apply(
                new FindOutThreadPoolSize(),

                ignite.cluster().forServers().nodes(),

                new IgniteReducer<Integer, Integer>() {
                    private AtomicInteger sum = new AtomicInteger();

                    // Callback for every job result.
                    @Override
                    public boolean collect(Integer len) {
                        sum.addAndGet(len);

                        // Return true to continue waiting until all results are received.
                        return true;
                    }

                    // Reduce all results into one.
                    @Override
                    public Integer reduce() {
                        return sum.get();
                    }
                });

        // Collect submitted endless futures for cleanup purposes.
        List<IgniteFuture> endlessFutures = new ArrayList<>();

        // Run asynchronously (TOTAL CLUSTER THREADS - NUMBER OF NODES) endless futures to hang all threads
        // in public thread pool on all the nodes, except one which will be left idle for fast task processing.
        for (int i = 0; i < totalPubThreadPoolSize - ignite.cluster().forServers().nodes().size(); i++) {
            IgniteFuture<Void> igniteFuture = compute.runAsync(new EndlessRunnable("I'm still running!"));

            endlessFutures.add(igniteFuture);
        }

        // We submit 100 tasks with execution time of 200ms to NODE 1
        // We submit 100 tasks with execution time of 1000ms to NODE 2
        // In the result: NODE 1 finishes 140 jobs. Why?
        List<IgniteFuture> swiftFutures = new ArrayList<>();

        for (int i = 0; i < 200; i++) {
            IgniteFuture<Void> igniteFuture = compute.runAsync(new SwiftRunnable(i, i + 1));
            swiftFutures.add(igniteFuture);
        }

        int total = 0;

        for (IgniteFuture fut : swiftFutures) {
            fut.get();
            total++;

            System.out.println("Finished " + total + " SwiftRunnable jobs so far.");
        }

        System.out.println("TOTAL: finished " + total + " SwiftRunnable jobs.");

        // Cancel all endless future to let the cluster accept new compute tasks.
        for (IgniteFuture fut : endlessFutures)
            fut.cancel();

        ignite.close();
    }

    /**
     * Closure that sums up number of threads in the public (compute) pool across all server nodes.
     */
    public static class FindOutThreadPoolSize implements IgniteClosure<ClusterNode, Integer> {
        @IgniteInstanceResource
        Ignite ignite;

        @Override
        public Integer apply(ClusterNode integer) {
            return ignite.configuration().getPublicThreadPoolSize();
        }
    }

    /**
     * A runnable that won't finish running unless cancelled.
     */
    public static class EndlessRunnable implements IgniteRunnable {
        @LoggerResource
        IgniteLogger logger;

        private final String t;

        public EndlessRunnable(String t) {
            this.t = t;
        }

        @Override
        public void run() {
            System.out.println(">>> Started running " + this.getClass().getCanonicalName() + " compute job/task.");

            while (true) {
                try {
                    Thread.sleep(2000);
                } catch (InterruptedException e) {
                    return;
                }

                log(logger, "This is a simple log message to let everyone know that EndlessRunnable did not finished the execution. " + t);
            }
        }
    }

    /**
     * Simple runnable that sleeps for 200 or 1000 ms and calculates some value a + b.
     */
    public static class SwiftRunnable implements IgniteRunnable {
        @LoggerResource
        IgniteLogger logger;

        private final int a;

        private final int b;

        public SwiftRunnable(int arg1, int arg2) {
            this.a = arg1;

            this.b = arg2;
        }

        @Override
        public void run() {
            System.out.println(">>> Started running " + this.getClass().getCanonicalName() + " compute job/task.");

            try {
                int mult = 1;

                if (a % 2 == 0)
                    mult = 5;

                Thread.sleep(200 * mult);
            } catch (InterruptedException e) {
                //e.printStackTrace();
            }

            log(logger, "a + b = " + (a + b));

            System.out.println(">>> Finished running " + this.getClass().getCanonicalName() + " compute job/task.");
        }
    }
}
