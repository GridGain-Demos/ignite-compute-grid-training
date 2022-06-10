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

import java.util.*;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.Ignition;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.compute.*;
import org.apache.ignite.resources.LoggerResource;
import org.gridgain.demo.model.SimpleMatrix;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * This demo shows a typical implementation of Map/Reduce compute job that extends
 * {@link ComputeTaskAdapter} class which is a convenience implementation of {@link ComputeTask}
 * that will wait for all task completion and will failover them in the case of exception.
 * <p>
 * We are performing a simple matrix multiplication and each compute task submitted as a part of map phase
 * represents a multiplication of column N and row M.
 */
public class IgniteMapReduce extends ComputeDemo {

    public static void main(String[] args) {
        Ignition.setClientMode(true);
        Ignite ignite = Ignition.start(CONFIG_PATH + "/ignite-config-node.xml");

        ClusterState clusterState = ignite.cluster().state();

        if (!clusterState.equals(ClusterState.ACTIVE))
            ignite.cluster().state(ClusterState.ACTIVE);

        double[][] values1 = new double[][] {{1, 1}, {2, 2}};
        double[][] values2 = new double[][] {{3, 3}, {4, 4}};

        SimpleMatrix sm1 = new SimpleMatrix(values1);
        SimpleMatrix sm2 = new SimpleMatrix(values2);

        SimpleMatrix res = ignite.compute().execute(GenericMapReduceTask.class, Arrays.asList(sm1, sm2));

        System.out.println("The result of matrix multiplication is: ");

        for ( int i = 0; i < res.getHeight(); i++ )
            System.out.println(Arrays.toString(res.getRow(i)));


        ignite.close();
    }

    /**
     * Task to count non-white-space characters in an argument string.
     */
    private static class GenericMapReduceTask extends ComputeTaskAdapter<List<SimpleMatrix>, SimpleMatrix> {

        private int h;
        private int w;

        @LoggerResource
        IgniteLogger logger;

        /**
         * Splits the received arguments, creates a child job for each word, and sends
         * these jobs to other nodes for processing. Each such job simply prints out the received word.
         *
         * @param nodes Nodes available for this task execution.
         * @param arg   String to split into words for processing. "asd abc def"
         * @return Map of jobs to nodes.
         */
        @NotNull
        @Override
        public Map<? extends ComputeJob, ClusterNode> map(List<ClusterNode> nodes, List<SimpleMatrix> arg) {
            Map<ComputeJob, ClusterNode> map = new HashMap<>();

            assert arg.size() == 2;

            SimpleMatrix m1 = arg.get(0);
            SimpleMatrix m2 = arg.get(1);

            Iterator<ClusterNode> it = nodes.iterator();

            if (!(m1.getWidth() == m2.getHeight())) {
                throw new IgniteException("Cannot multiply the matrices.");
            }

            this.h = m1.getHeight();
            this.w = m2.getWidth();

            // Slice matrices to rows (i) and columns (j).
            for (int i = 0; i < m1.getHeight(); i++) {
                for (int j = 0; j < m2.getWidth(); j++) {
                    double[] col = m1.getCol(i);
                    double[] row = m2.getRow(j);

                    if (!it.hasNext())
                        it = nodes.iterator();

                    ClusterNode node = it.next();

                    final int idxa = i;
                    final int idxb = j;

                    // Submit compute job for each pair of row/col that has to be multiplied and aggregated.
                    map.put(new ComputeJobAdapter() {
                        @Nullable
                        @Override
                        public Object execute() {
                            log(logger, ">>> Started running matrix multiplication of row/col compute job.");
                            double sum = 0.0d;

                            for (int m = 0; m < col.length; m++) {
                                    sum += col[m] * row[m];
                            }

                            SimpleMatrix s = new SimpleMatrix(m1.getHeight(), m2.getWidth());
                            s.setVal(idxa, idxb, sum);
                            log(logger, "Multiplication of " + Arrays.toString(col) + " * " + Arrays.toString(row) + " = " + sum + ".");

                            log(logger, ">>> Finished running matrix multiplication of row/col compute job.");

                            return s;
                        }
                    }, node);
                }
            }

            return map;
        }

        /**
         * {@inheritDoc}
         */
        @Nullable
        @Override
        public SimpleMatrix reduce(List<ComputeJobResult> results) {
            SimpleMatrix sum = new SimpleMatrix(h, w);

            for (ComputeJobResult res : results)
                sum.add(res.<SimpleMatrix>getData());

            return sum;
        }


    }
}
