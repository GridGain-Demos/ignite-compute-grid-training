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
import org.apache.ignite.IgniteCluster;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.Ignition;
import org.apache.ignite.cluster.BaselineNode;
import org.apache.ignite.cluster.ClusterGroup;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.lang.IgniteRunnable;
import org.apache.ignite.resources.IgniteInstanceResource;
import org.apache.ignite.resources.LoggerResource;

import java.util.Collection;

/**
 * This demo contains the simplest implementation of {@link IgniteRunnable} that is using the resources of the node
 * where the computation arrives for execution with the help of {@link IgniteInstanceResource} annotation.
 * <p>
 * As it shown here, we can also create a dedicated {@link ClusterGroup} to be the subset of nodes that will
 * actually run the computation, in this case it is only the coordinator node which is determined as
 * {@link IgniteCluster#forOldest()}.
 */
public class SimpleComputeClosure extends ComputeDemo {

    public static void main(String[] args) {
        Ignition.setClientMode(true);
        Ignite ignite = Ignition.start(CONFIG_PATH + "/ignite-config-node.xml");

        ClusterState clusterState = ignite.cluster().state();

        if (!clusterState.equals(ClusterState.ACTIVE))
            ignite.cluster().state(ClusterState.ACTIVE);

        // Run the simple job on all cluster nodes (N jobs, where N = number of nodes).
        ignite.compute().broadcast(new BroadcastRunnable("It was broadcasted."));

        // Run the simple job on all cluster nodes (N jobs, where N = number of nodes) within custom thread pool.
        ignite.compute().withExecutor("customExecutor")
                .broadcast(new BroadcastRunnable("It was broadcasted."));

        // Find the oldest node = cluster coordinator.
        ClusterGroup crdClusterGroup = ignite.cluster().forOldest();

        // Run the job on coordinator cluster group, will be executed only on specified node.
        ignite.compute(crdClusterGroup).broadcast(new BroadcastRunnable("It was sent only for coordinator."));

        // Run the job to make each node print consistent ids of all other server nodes.
        ignite.compute().broadcast(new PrintOtherNodeConsistentIdRunnable());

        ignite.close();
    }

    public static class BroadcastRunnable implements IgniteRunnable {
        @LoggerResource
        IgniteLogger logger;

        private final String t;

        public BroadcastRunnable(String t) {
            this.t = t;
        }

        @Override
        public void run() {
            System.out.println(">>> Started running " + this.getClass().getCanonicalName() + " compute job/task.");

            log(logger, "Thread: " + Thread.currentThread().getName()
                    + ": This is a simple log message. " + t);

            System.out.println(">>> Finished running " + this.getClass().getCanonicalName() + " compute job/task.");
        }
    }

    public static class PrintOtherNodeConsistentIdRunnable implements IgniteRunnable {
        @LoggerResource
        IgniteLogger logger;

        @IgniteInstanceResource
        Ignite ignite;

        @Override
        public void run() {
            System.out.println(">>> Started running " + this.getClass().getCanonicalName() + " compute job/task.");

            log(logger, "Thread: " + Thread.currentThread().getName() + ": I will print all other nodes in topology now!");

            Collection<BaselineNode> baselineNodes = ignite.cluster().currentBaselineTopology();

            for (BaselineNode node : baselineNodes) {
                if (!node.consistentId().equals(ignite.cluster().localNode().consistentId()))
                    log(logger, "Hey, I've found a node that is not me: " + node.consistentId().toString());
            }

            System.out.println(">>> Finished running " + this.getClass().getCanonicalName() + " compute job/task.");
        }
    }

}
