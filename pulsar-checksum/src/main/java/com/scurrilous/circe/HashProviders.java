/*******************************************************************************
 * Copyright 2014 Trevor Robinson
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 ******************************************************************************/
package com.scurrilous.circe;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.EnumSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.ServiceLoader;
import java.util.SortedMap;
import java.util.TreeMap;

/**
 * Static utility methods for discovering {@link HashProvider} instances.
 */
public final class HashProviders {

    static final Collection<HashProvider> ALL_PROVIDERS = getAllProviders();

    private static Collection<HashProvider> getAllProviders() {
        final ServiceLoader<HashProvider> loader = ServiceLoader.load(HashProvider.class);
        final LinkedList<HashProvider> providers = new LinkedList<>();
        for (final HashProvider provider : loader)
            providers.add(provider);
        return Collections.unmodifiableList(new ArrayList<>(providers));
    }

    private HashProviders() {
    }

    /**
     * Returns an iterator over all known {@link HashProvider} instances.
     * 
     * @return an iterator over all HashProviders
     */
    public static Iterator<HashProvider> iterator() {
        return ALL_PROVIDERS.iterator();
    }

    /**
     * Returns the best hash provider supporting at least a stateful
     * implementation of a hash function with the given parameters.
     * 
     * @param params the parameters defining the hash function
     * @return the best hash provider for the given parameters
     * @throws UnsupportedOperationException if no provider supports the
     *             parameters
     */
    public static HashProvider best(HashParameters params) {
        return best(params, EnumSet.of(HashSupport.STATEFUL));
    }

    /**
     * Returns the best hash provider supporting at least the given flags for a
     * hash function with the given parameters.
     * 
     * @param params the parameters defining the hash function
     * @param required the required support flags for a provider to be
     *            considered
     * @return the best hash provider for the given parameters
     * @throws UnsupportedOperationException if no provider supports the
     *             parameters
     */
    public static HashProvider best(HashParameters params, EnumSet<HashSupport> required) {
        HashProvider result = null;
        EnumSet<HashSupport> resultSupport = null;
        for (final HashProvider provider : ALL_PROVIDERS) {
            final EnumSet<HashSupport> support = provider.querySupport(params);
            if (support.containsAll(required) &&
                    (result == null || HashSupport.compare(support, resultSupport) < 0)) {
                result = provider;
                resultSupport = support;
            }
        }
        if (result == null)
            throw new UnsupportedOperationException();
        return result;
    }

    /**
     * Returns a map of hash providers supporting at least a stateful
     * implementation of a hash function with the given parameters.
     * 
     * @param params the parameters defining the hash function
     * @return a sorted map of hash support flags to hash providers
     */
    public static SortedMap<EnumSet<HashSupport>, HashProvider> search(HashParameters params) {
        return search(params, EnumSet.of(HashSupport.STATEFUL));
    }

    /**
     * Returns a map of hash providers supporting at least the given flags for a
     * hash function with the given parameters.
     * 
     * @param params the parameters defining the hash function
     * @param required the required support flags for a provider to be included
     * @return a sorted map of hash support flags to hash providers
     */
    public static SortedMap<EnumSet<HashSupport>, HashProvider> search(HashParameters params,
            EnumSet<HashSupport> required) {
        final SortedMap<EnumSet<HashSupport>, HashProvider> result = new TreeMap<>(
                new HashSupport.SetComparator());
        for (final HashProvider provider : ALL_PROVIDERS) {
            final EnumSet<HashSupport> support = provider.querySupport(params);
            if (support.containsAll(required))
                result.put(support, provider);
        }
        return result;
    }
}
