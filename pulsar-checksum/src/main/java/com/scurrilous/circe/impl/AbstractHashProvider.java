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
package com.scurrilous.circe.impl;

import java.util.EnumSet;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;

import com.scurrilous.circe.Hash;
import com.scurrilous.circe.HashParameters;
import com.scurrilous.circe.HashProvider;
import com.scurrilous.circe.HashSupport;
import com.scurrilous.circe.IncrementalIntHash;
import com.scurrilous.circe.IncrementalLongHash;
import com.scurrilous.circe.StatefulHash;
import com.scurrilous.circe.StatefulIntHash;
import com.scurrilous.circe.StatefulLongHash;
import com.scurrilous.circe.StatelessHash;
import com.scurrilous.circe.StatelessIntHash;
import com.scurrilous.circe.StatelessLongHash;

/**
 * Base implementation for hash function providers.
 * 
 * @param <P> base supported hash parameters type
 */
public abstract class AbstractHashProvider<P extends HashParameters> implements HashProvider {

    private final Class<P> parametersClass;

    /**
     * Constructs a new {@link AbstractHashProvider} with the given base
     * parameters class.
     * 
     * @param parametersClass the base hash parameters class supported
     */
    protected AbstractHashProvider(Class<P> parametersClass) {
        this.parametersClass = parametersClass;
    }

    @Override
    public final EnumSet<HashSupport> querySupport(HashParameters params) {
        if (!parametersClass.isAssignableFrom(params.getClass()))
            return EnumSet.noneOf(HashSupport.class);
        return querySupportTyped(parametersClass.cast(params));
    }

    /**
     * Implemented by subclasses to provide information about the available
     * implementations corresponding to the given hash algorithm parameters.
     * Called by {@link #querySupport} if the hash parameters match the base
     * type supported by this provider.
     * 
     * @param params the hash algorithm parameters
     * @return a set of flags indicating the level of support
     */
    protected abstract EnumSet<HashSupport> querySupportTyped(P params);

    /**
     * Requests a hash function using the given parameters and support flags.
     * This method is only responsible for checking support flags returned by
     * {@link #querySupportTyped}.
     * <p>
     * To support caching of stateless hash functions, call
     * {@link #getCacheable} from this method and implement
     * {@link #createCacheable}.
     * 
     * @param params the hash algorithm parameters
     * @param required the required hash support flags
     * @return a hash function
     * @throws UnsupportedOperationException if this provider cannot support the
     *             given parameters
     */
    protected abstract Hash get(P params, EnumSet<HashSupport> required);

    /**
     * Called by implementations that support caching of stateless hash
     * functions when a cached instance is desired. If a cached instance is not
     * available, this method calls {@link #createCacheable} to create one,
     * which is then cached (if caching is available).
     * 
     * @param params the hash algorithm parameters
     * @param required the required hash support flags
     * @return a hash function
     * @throws UnsupportedOperationException if this provider cannot support the
     *             given parameters
     */
    protected final Hash getCacheable(final P params, final EnumSet<HashSupport> required) {
        if (HashCacheLoader.hasCache()) {
            final HashCache cache = HashCacheLoader.getCache();
            try {
                return cache.get(params, required, new Callable<Hash>() {
                    @Override
                    public Hash call() throws Exception {
                        return createCacheable(params, required);
                    }
                });
            } catch (ExecutionException e) {
                final Throwable cause = e.getCause();
                if (cause instanceof RuntimeException)
                    throw (RuntimeException) cause;
                throw new UnsupportedOperationException(e);
            }
        }
        return createCacheable(params, required);
    }

    /**
     * Called by {@link #getCacheable} to create new cacheable stateless hash
     * functions. The default implementation simply throws
     * {@link UnsupportedOperationException}.
     * 
     * @param params the hash algorithm parameters
     * @param required the required hash support flags
     * @return a stateless hash function
     * @throws UnsupportedOperationException if this provider cannot support the
     *             given parameters
     */
    protected StatelessHash createCacheable(P params, EnumSet<HashSupport> required) {
        throw new UnsupportedOperationException();
    }

    private Hash castAndGet(HashParameters params, EnumSet<HashSupport> required) {
        if (!parametersClass.isAssignableFrom(params.getClass()))
            throw new UnsupportedOperationException();
        return get(parametersClass.cast(params), required);
    }

    @Override
    public StatefulHash createStateful(HashParameters params) {
        final Hash hash = castAndGet(params, EnumSet.of(HashSupport.STATEFUL));
        if (hash instanceof StatefulHash)
            return (StatefulHash) hash;
        if (hash instanceof StatelessHash)
            return ((StatelessHash) hash).createStateful();
        throw new UnsupportedOperationException();
    }

    @Override
    public StatelessIntHash getStatelessInt(HashParameters params) {
        final Hash hash = castAndGet(params, EnumSet.of(HashSupport.INT_SIZED));
        if (hash instanceof StatelessIntHash)
            return (StatelessIntHash) hash;
        if (hash instanceof StatefulIntHash)
            return ((StatefulIntHash) hash).asStateless();
        throw new UnsupportedOperationException();
    }

    @Override
    public StatelessLongHash getStatelessLong(HashParameters params) {
        final Hash hash = castAndGet(params, EnumSet.of(HashSupport.LONG_SIZED));
        if (hash instanceof StatelessLongHash)
            return (StatelessLongHash) hash;
        if (hash instanceof StatefulLongHash)
            return ((StatefulLongHash) hash).asStateless();
        if (hash instanceof StatelessIntHash)
            return new IntStatelessLongHash((StatelessIntHash) hash);
        if (hash instanceof StatefulIntHash)
            return new IntStatelessLongHash(((StatefulIntHash) hash).asStateless());
        throw new UnsupportedOperationException();
    }

    @Override
    public IncrementalIntHash getIncrementalInt(HashParameters params) {
        final Hash hash = castAndGet(params,
                EnumSet.of(HashSupport.INT_SIZED, HashSupport.STATELESS_INCREMENTAL));
        if (hash instanceof IncrementalIntHash)
            return (IncrementalIntHash) hash;
        throw new UnsupportedOperationException();
    }

    @Override
    public IncrementalLongHash getIncrementalLong(HashParameters params) {
        final Hash hash = castAndGet(params,
                EnumSet.of(HashSupport.LONG_SIZED, HashSupport.STATELESS_INCREMENTAL));
        if (hash instanceof IncrementalLongHash)
            return (IncrementalLongHash) hash;
        throw new UnsupportedOperationException();
    }
}
