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
import org.apache.ignite.cluster.ClusterGroup;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.compute.ComputeJobContext;
import org.apache.ignite.compute.ComputeTaskFuture;
import org.apache.ignite.lang.*;
import org.apache.ignite.resources.IgniteInstanceResource;
import org.apache.ignite.resources.JobContextResource;
import org.jetbrains.annotations.Nullable;

import java.math.BigInteger;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentMap;

/**
 * This example demonstrates how to use continuation feature of Ignite by
 * performing the distributed recursive calculation of {@code 'Fibonacci'}
 * numbers on the cluster. Continuations functionality is exposed via {@link ComputeJobContext#holdcc()}
 * and {@link ComputeJobContext#callcc()} method calls in
 * {@link org.gridgain.demo.compute.FibonacciContinuationExample.FibonacciClosure}
 * class.
 * <p>
 * Remote nodes should always be started with special configuration file which
 * enables P2P class loading: {@code 'ignite.{sh|bat} config/ignite-config-node.xml'}.
 */
public final class FibonacciContinuationExample extends ComputeDemo {
    /**
     * Executes example.
     *
     * @param args Command line arguments, none required.
     * @throws IgniteException If example execution failed.
     */
    public static void main(String[] args) throws IgniteException, InterruptedException {
        Ignition.setClientMode(true);

        try (Ignite ignite = Ignition.start(CONFIG_PATH + "/ignite-config-node.xml")) {
            ClusterState clusterState = ignite.cluster().state();

            if (!clusterState.equals(ClusterState.ACTIVE))
                ignite.cluster().state(ClusterState.ACTIVE);

            System.out.println();
            System.out.println("Compute Tiling continuation example started.");

            long N = 120;

            final UUID exampleNodeId = ignite.cluster().localNode().id();

            // Create a filter to remove the local node from the cluster group that will execute the task.
            final IgnitePredicate<ClusterNode> nodeFilter = new IgnitePredicate<ClusterNode>() {
                @Override
                public boolean apply(ClusterNode n) {
                    // Give preference to remote nodes.
                    return ignite.cluster().forRemotes().nodes().isEmpty() || !n.id().equals(exampleNodeId);
                }
            };

            // Run naive closure implementation for N = 3, must finish.
            N = 3;

            long start = System.currentTimeMillis();

            BigInteger fibv1 = ignite.compute(ignite.cluster().forPredicate(nodeFilter)).apply(
                    new FibonacciClosureBad(nodeFilter), N);

            long duration = System.currentTimeMillis() - start;

            printFibonacciCalculated(N, fibv1, duration);

            if (fibv1.compareTo(BigInteger.ZERO) < 0)
                return;

            BigInteger fibv2 = BigInteger.valueOf(-1L);

            start = System.currentTimeMillis();

            // Add future cleanup task beforehand, as thread pools will starve after next Fibonacci task submission.
            IgniteFuture<Void> broadcastAsyncFut = ignite.compute(ignite.cluster().forServers()).broadcastAsync(new CleanupTaskFutures());

            // Run naive closure implementation for N = 120. Won't ever finish, so we must cancel the submitted tasks.
            N = 120;

            IgniteFuture<BigInteger> fibFuture = ignite.compute(ignite.cluster().forPredicate(nodeFilter)).applyAsync(
                    new FibonacciClosureBad(nodeFilter), N);

            // Sleep  30 sec to let closures run.
            Thread.sleep(30000);

            // Cancel the futures and future cleanup task.
            fibFuture.cancel();
            broadcastAsyncFut.cancel();

            duration = System.currentTimeMillis() - start;

            printFibonacciCalculated(N, fibv2, duration);

            // Run continuation closure implementation for N = 3.
            N = 3;

            start = System.currentTimeMillis();

            BigInteger fibv3 = ignite.compute(ignite.cluster().forPredicate(nodeFilter)).apply(
                    new FibonacciClosureBad(nodeFilter), N);

            duration = System.currentTimeMillis() - start;

            printFibonacciCalculated(N, fibv3, duration);

            // Run continuation closure implementation for N = 120. Will correctly finish and print
            // the correct answer 'fib(120) = 5358359254990966640871840'.
            N = 120;

            start = System.currentTimeMillis();

            BigInteger fib = ignite.compute(ignite.cluster().forPredicate(nodeFilter)).apply(
                    new FibonacciClosure(nodeFilter), N);

            duration = System.currentTimeMillis() - start;

            printFibonacciCalculated(N, fib, duration);
            System.out.println(">>> If you re-run this example w/o stopping remote nodes - the performance will");
            System.out.println(">>> increase since intermediate results are pre-cache on remote nodes.");
            System.out.println(">>> You should see prints out every recursive Tiling calc execution on cluster nodes.");
            System.out.println(">>> Check remote nodes for output.");
        }
    }

    private static void printFibonacciCalculated(long n, BigInteger fibv1, long duration) {
        System.out.println();
        System.out.println(">>> Finished executing fibonacci for '" + n + "' in " + duration + " ms.");
        System.out.println(">>> Fibonacci number '" + n + "' is '" + fibv1.longValue() + "'.");
    }

    /**
     * Correct closure to execute.
     */
    private static class FibonacciClosure implements IgniteClosure<Long, BigInteger> {
        /**
         * Future for spawned task.
         */
        private IgniteFuture<BigInteger> fn1fut;

        /**
         * Future for spawned task.
         */
        private IgniteFuture<BigInteger> fn2fut;
        ;

        /**
         * Auto-inject job context.
         */
        @JobContextResource
        private ComputeJobContext jobCtx;

        /**
         * Auto-inject ignite instance.
         */
        @IgniteInstanceResource
        private Ignite ignite;

        /**
         * Predicate.
         */
        private final IgnitePredicate<ClusterNode> nodeFilter;

        /**
         * @param nodeFilter Predicate to filter nodes.
         */
        FibonacciClosure(IgnitePredicate<ClusterNode> nodeFilter) {
            this.nodeFilter = nodeFilter;
        }

        /**
         * {@inheritDoc}
         */
        @Nullable
        @Override
        public BigInteger apply(Long n) {
            System.out.println(">>> Started running " + this.getClass().getCanonicalName() + " compute job/task.");

            if (fn1fut == null || fn2fut == null) {
                System.out.println();
                System.out.println(">>> Starting execution of number of fibonacci number: " + n);

                // Make sure n is not negative.
                n = Math.abs(n);

                if (n <= 1)
                    return BigInteger.valueOf(n);

                ConcurrentMap<Long, IgniteFuture<BigInteger>> locMap = ignite.cluster().nodeLocalMap();

                // Check if value is cached in node-local-map first.
                fn1fut = locMap.get(n - 1);
                fn2fut = locMap.get(n - 2);

                ClusterGroup p = ignite.cluster().forPredicate(nodeFilter);

                IgniteCompute compute = ignite.compute(p);

                // If future is not cached in node-local-map, cache it.
                if (fn1fut == null) {
                    IgniteFuture<BigInteger> futVal = compute.applyAsync(
                            new FibonacciClosure(nodeFilter), n - 1);

                    fn1fut = locMap.putIfAbsent(n - 1, futVal);

                    if (fn1fut == null)
                        fn1fut = futVal;
                }

                // If future is not cached in node-local-map, cache it.
                if (fn2fut == null) {
                    IgniteFuture<BigInteger> futVal = compute.applyAsync(
                            new FibonacciClosure(nodeFilter), n - 2);

                    fn2fut = locMap.putIfAbsent(n - 2, futVal);

                    if (fn2fut == null)
                        fn2fut = futVal;
                }

                // If futures are not done, then wait asynchronously for the result
                if (!fn1fut.isDone() || !fn2fut.isDone()) {
                    IgniteInClosure<IgniteFuture<BigInteger>> lsnr = new IgniteInClosure<IgniteFuture<BigInteger>>() {
                        @Override
                        public void apply(IgniteFuture<BigInteger> f) {
                            // If both futures are done, resume the continuation.
                            if (fn1fut.isDone() && fn2fut.isDone())
                                // CONTINUATION:
                                // =============
                                // Resume suspended job execution.
                                jobCtx.callcc();
                        }
                    };

                    // CONTINUATION:
                    // =============
                    // Hold (suspend) job execution.
                    // It will be resumed in listener above via 'callcc()' call
                    // once both futures are done.
                    jobCtx.holdcc();

                    // Attach the same listener to both futures.
                    fn1fut.listen(lsnr);
                    fn2fut.listen(lsnr);

                    return null;
                }
            }

            assert fn1fut.isDone() && fn2fut.isDone();

            BigInteger fn1 = fn1fut.get();
            BigInteger fn2 = fn2fut.get();

            System.out.println(">>> Finished running " + this.getClass().getCanonicalName() + " compute job/task for number " + n);

            // Return cached results.
            return fn1.add(fn2);
        }
    }

    /**
     * Naive closure implementation to execute, will block the thread pools for sufficiently large values of N submitted.
     */
    private static class FibonacciClosureBad implements IgniteClosure<Long, BigInteger> {
        /**
         * Auto-inject ignite instance.
         */
        @IgniteInstanceResource
        private Ignite ignite;

        /**
         * Predicate.
         */
        private final IgnitePredicate<ClusterNode> nodeFilter;

        /**
         * @param nodeFilter Predicate to filter nodes.
         */
        public FibonacciClosureBad(IgnitePredicate<ClusterNode> nodeFilter) {
            this.nodeFilter = nodeFilter;
        }

        /**
         * {@inheritDoc}
         */
        @Nullable
        @Override
        public BigInteger apply(Long n) {

            System.out.println();
            System.out.println(">>> Starting execution of number of fibonacci number: " + n);

            // Make sure n is not negative.
            n = Math.abs(n);

            if (n <= 2)
                return BigInteger.valueOf(n);

            ClusterGroup p = ignite.cluster().forPredicate(nodeFilter);

            IgniteCompute compute = ignite.compute(p);

            //calculate fib(n-1)
            BigInteger fn1 = compute.apply(
                    new FibonacciClosureBad(nodeFilter), n - 1);

            //calculate fib(n-2)
            BigInteger fn2 = compute.apply(
                    new FibonacciClosureBad(nodeFilter), n - 2);

            System.out.println(">>> Finished execution of fibonacci number " + n + ".");

            // Return cached results.
            return fn1.add(fn2);
        }
    }

    public static class CleanupTaskFutures implements IgniteRunnable {
        @IgniteInstanceResource
        Ignite ignite;

        @Override
        public void run() {
            int i = 0;

            while ( i < 60 ) {
                try {
                    Thread.sleep(1000);

                    int size = ignite.compute().activeTaskFutures().size();

                    System.out.println("Currently there are: " + size + " active task futures.");
                } catch (InterruptedException e) {
                    e.printStackTrace();

                    return;
                }

                i++;
            }
            Map<IgniteUuid, ComputeTaskFuture<Object>> igniteUuidComputeTaskFutureMap = ignite.compute().activeTaskFutures();

            for (Map.Entry<IgniteUuid, ComputeTaskFuture<Object>> computeTaskFutureEntry : igniteUuidComputeTaskFutureMap.entrySet()) {
                computeTaskFutureEntry.getValue().cancel();
            }
        }
    }
}