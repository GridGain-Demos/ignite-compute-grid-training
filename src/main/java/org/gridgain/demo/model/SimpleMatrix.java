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

package org.gridgain.demo.model;

public class SimpleMatrix {

    private double[][] values;

    private int h;
    private int w;

    public SimpleMatrix(double[][] values) {

        this.values = values;

        h = values.length;
        w = values[0].length;
    }

    // create new matrix with given dimensions
    public SimpleMatrix(int k, int m) {
        if (k == 0 || m == 0)
            return;

        this.values = new double[k][m];
        this.w = m;
        this.h = k;
    }

    public int getHeight() {
        return h;
    }

    public int getWidth() {
        return w;
    }

    public double[] getRow(int n) {
        double[] col = new double[w];

        for (int i = 0; i < w; i++) {
            col[i] = values[n][i];
        }

        return col;
    }

    public double[] getCol(int n) {
        double[] row = new double[h];

        for (int i = 0; i < h; i++) {
            row[i] = values[i][n];
        }

        return row;
    }

    public void setVal(int n, int m, double val) {
        this.values[n][m] = val;
    }

    public double getVal(int n, int m) {
        return values[n][m];
    }

    public SimpleMatrix add(SimpleMatrix s) {
        for (int i = 0; i < h; i++) {
            for (int j = 0; j < w; j++) {
                this.values[i][j] += s.getVal(i, j);
            }
        }

        return this;
    }
}
