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

import java.math.BigInteger;
import java.util.UUID;
import java.util.concurrent.ConcurrentMap;

import org.apache.ignite.*;
import org.apache.ignite.cluster.ClusterGroup;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.compute.ComputeJobContext;
import org.apache.ignite.lang.*;
import org.apache.ignite.resources.IgniteInstanceResource;
import org.apache.ignite.resources.JobContextResource;
import org.jetbrains.annotations.Nullable;

/**
 * This example demonstrates how to use continuation feature of Ignite by
 * performing the distributed recursive calculation of numbers of {@code 'Tilings'} of Nx2 board
 * on the cluster. Continuations functionality is exposed via {@link ComputeJobContext#holdcc()}
 * and {@link ComputeJobContext#callcc()} method calls in
 * {@link org.gridgain.demo.compute.TilingContinuationExample.DominoTrominoTilingClosure}
 * class.
 * <p>
 * Remote nodes should always be started with special configuration file which
 * enables P2P class loading: {@code 'ignite.{sh|bat} config/ignite-config-node.xml'}.
 */
public final class TilingContinuationExample extends ComputeDemo {
    /**
     * Executes example.
     *
     * @param args Command line arguments, none required.
     * @throws IgniteException If example execution failed.
     */
    public static void main(String[] args) throws IgniteException {
        Ignition.setClientMode(true);

        try (Ignite ignite = Ignition.start(CONFIG_PATH + "/ignite-config-node.xml")) {
            ClusterState clusterState = ignite.cluster().state();

            if (!clusterState.equals(ClusterState.ACTIVE))
                ignite.cluster().state(ClusterState.ACTIVE);

            System.out.println();
            System.out.println("Compute Tiling continuation example started.");

            long N = 120;

            final UUID exampleNodeId = ignite.cluster().localNode().id();

            // Create the caches.
            IgniteCache<Long, BigInteger> fCache = ignite.getOrCreateCache("F");
            IgniteCache<Long, BigInteger> pCache = ignite.getOrCreateCache("P");

            fCache.put(0L, BigInteger.valueOf(0L));
            fCache.put(1L, BigInteger.valueOf(1L));
            fCache.put(2L, BigInteger.valueOf(2L));

            pCache.put(0L, BigInteger.valueOf(0L));
            pCache.put(1L, BigInteger.valueOf(1L));
            pCache.put(2L, BigInteger.valueOf(1L));

            // Create a filter to remove the local node from the cluster group that will execute the task.
            final IgnitePredicate<ClusterNode> nodeFilter = new IgnitePredicate<ClusterNode>() {
                @Override public boolean apply(ClusterNode n) {
                    // Give preference to remote nodes.
                    return ignite.cluster().forRemotes().nodes().isEmpty() || !n.id().equals(exampleNodeId);
                }
            };

            long start = System.currentTimeMillis();

            BigInteger fib = ignite.compute(ignite.cluster().forPredicate(nodeFilter)).apply(
                    new DominoTrominoTilingClosure(nodeFilter), N);

            long duration = System.currentTimeMillis() - start;

            System.out.println();
            System.out.println(">>> Finished executing tiling for '" + N + "' in " + duration + " ms.");
            System.out.println(">>> Tiling options count for input number '" + N + "' is '" + fib + "'.");
            System.out.println(">>> If you re-run this example w/o stopping remote nodes - the performance will");
            System.out.println(">>> increase since intermediate results are pre-cache on remote nodes.");
            System.out.println(">>> You should see prints out every recursive Tiling calc execution on cluster nodes.");
            System.out.println(">>> Check remote nodes for output.");
        }
    }

    /**
     * Closure to execute.
     */
    private static class DominoTrominoTilingClosure implements IgniteClosure<Long, BigInteger> {
        /** Future for spawned task. */
        private IgniteFuture<BigInteger> fn1fut;

        /** Future for spawned task. */
        private IgniteFuture<BigInteger> fn2fut;;

        /** Auto-inject job context. */
        @JobContextResource
        private ComputeJobContext jobCtx;

        /** Auto-inject ignite instance. */
        @IgniteInstanceResource
        private Ignite ignite;

        /** Predicate. */
        private final IgnitePredicate<ClusterNode> nodeFilter;

        /**
         * @param nodeFilter Predicate to filter nodes.
         */
        DominoTrominoTilingClosure(IgnitePredicate<ClusterNode> nodeFilter) {
            this.nodeFilter = nodeFilter;
        }

        /** {@inheritDoc} */
        @Nullable @Override public BigInteger apply(Long n) {
            System.out.println(">>> Started running " + this.getClass().getCanonicalName() + " compute job/task.");

            IgniteCache<Long, BigInteger> fCache = ignite.cache("F");
            IgniteCache<Long, BigInteger> pCache = ignite.cache("P");

            if (fn1fut == null || fn2fut == null) {
                System.out.println();
                System.out.println(">>> Starting execution of number of ways to tile for number: " + n);

                // Make sure n is not negative.
                n = Math.abs(n);

                if (n <= 2)
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
                            new DominoTrominoTilingClosure(nodeFilter), n - 1);

                    fn1fut = locMap.putIfAbsent(n - 1, futVal);

                    if (fn1fut == null)
                        fn1fut = futVal;
                }

                // If future is not cached in node-local-map, cache it.
                if (fn2fut == null) {
                    IgniteFuture<BigInteger> futVal = compute.applyAsync(
                            new DominoTrominoTilingClosure(nodeFilter), n - 2);

                    fn2fut = locMap.putIfAbsent(n - 2, futVal);

                    if (fn2fut == null)
                        fn2fut = futVal;
                }

                // If futures are not done, then wait asynchronously for the result
                if (!fn1fut.isDone() || !fn2fut.isDone()) {
                    IgniteInClosure<IgniteFuture<BigInteger>> lsnr = new IgniteInClosure<IgniteFuture<BigInteger>>() {
                        @Override public void apply(IgniteFuture<BigInteger> f) {
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

            // width = n, height = 2
            // f(n) = f(n-1)  + f(n-2) + p(n-1) * 2
            BigInteger pn1 = pCache.get(n-1);

            BigInteger fn = fn1.add(fn2).add(BigInteger.valueOf(2L).multiply(pn1));
            BigInteger pn = pn1.add(fn2);

            UUID localNodeUUID = ignite.cluster().localNode().id();

            System.out.println("Local node with id " + localNodeUUID.toString() + " is computing number of ways to tile for length of the tiling surface = " + n +
                    ", f(n-1) = " + fn1.toString() + ", f(n-2) = " + fn2.toString() + ", p(n-1) = " + pn1.toString() + "\n" +
                    "F(n) = " + fn.toString() + "\n" +
                    "P(n) = " + pn.toString());

            fCache.put(n, fn);
            pCache.put(n, pn);

            System.out.println(">>> Finished running " + this.getClass().getCanonicalName() + " compute job/task for number " + n);

            // Return cached results.
            return fn;
        }
    }
}