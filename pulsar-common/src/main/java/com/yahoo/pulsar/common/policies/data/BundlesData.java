/**
 * Copyright 2016 Yahoo Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.yahoo.pulsar.common.policies.data;

import java.util.List;

import com.google.common.base.Objects;

public class BundlesData {
    public List<String> boundaries;
    public int numBundles;

    public BundlesData() {
        this.boundaries = null;
        this.numBundles = 0;
    }

    public BundlesData(List<String> boundaries) {
        this.boundaries = boundaries;
        boundaries.sort(null);
        this.numBundles = boundaries.size() - 1;
    }

    public BundlesData(int numBundles) {
        this.boundaries = null;
        this.numBundles = numBundles;
    }

    public List<String> getBoundaries() {
        return boundaries;
    }

    public void setBoundaries(List<String> boundaries) {
        this.boundaries = boundaries;
        boundaries.sort(null);
    }

    public int getNumBundles() {
        return numBundles;
    }

    public void setNumBundles(int numBundles) {
        this.numBundles = numBundles;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof BundlesData) {
            BundlesData other = (BundlesData) obj;
            return Objects.equal(boundaries, other.boundaries);
        }

        return false;
    }

    @Override
    public String toString() {
        return Objects.toStringHelper(this).add("numBundles", numBundles).add("boundaries", boundaries).toString();
    }

}
