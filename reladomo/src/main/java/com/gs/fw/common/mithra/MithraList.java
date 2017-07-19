
/*
 Copyright 2016 Goldman Sachs.
 Licensed under the Apache License, Version 2.0 (the "License");
 you may not use this file except in compliance with the License.
 You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing,
 software distributed under the License is distributed on an
 "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 KIND, either express or implied.  See the License for the
 specific language governing permissions and limitations
 under the License.
 */

package com.gs.fw.common.mithra;

import com.gs.fw.common.mithra.finder.Operation;
import com.gs.fw.common.mithra.util.DoWhileProcedure;
import com.gs.fw.common.mithra.util.Filter;
import com.gs.fw.finder.DomainList;
import java.util.RandomAccess;

public interface MithraList<E> extends DomainList<E>, RandomAccess
{

    /**
     * force this list to resolve its operation. Normally, the operation is not resolved until necessary.
     */
    public void forceResolve();

    /**
     * force this list to be re-read from the database. Works for both operation based and simple lists.
     * It has no effect on a list of detached objects.
     */
    public void forceRefresh();

    /**
     * If this is set to true, the list will not be resolved from cache.
     *
     * @param bypassCache
     */
    public void setBypassCache(boolean bypassCache);

    /**
     * If this is set to true, the generated sql will use implicit joins
     *
     * @param forceImplicitJoin
     */
    public void setForceImplicitJoin(boolean forceImplicitJoin);

    /**
     * @return the operation that list was constructed from, or null.
     */
    public Operation getOperation();

    /**
     * @return true if the list was constructed with an operation.
     */
    public boolean isOperationBased();

    /**
     * Iterates through the list using a {@link com.gs.fw.common.mithra.list.cursor.Cursor}.
     * <p/>
     * This method executes the closure given as an argument for every items of
     * the list. It stops as soon as the list is empty or when the closure's
     * {@link DoWhileProcedure#execute(Object)} operation returns false.
     * This method should be used for huge lists constructed from operations
     * (and to be effective on entities configured with a partial cache).
     * It doesn't load all the objects in memory but loads them one by one
     * in the weak part of the cache (while iterating).
     * The deepFetch operation is not supported and should not be invoked prior
     * to using this operation (use {@link #iterator()} if deepfetch is needed).
     *
     * @param closure The code that will be executed for each element of the list.
     * @see #iterator()
     *
     */
    public void forEachWithCursor(DoWhileProcedure closure);

    public void forEachWithCursor(DoWhileProcedure closure, Operation filter);

    public void forEachWithCursor(DoWhileProcedure closure, Filter filter);

    /**
     * Clears the list of resolved references.</p>
     * This method will clear its internal list of results making all the referenced objects
     * eligible for garbage collection.
     * <p/>
     * Be aware that calling any method on a list (i.e. size()) after calling this method will cause,
     * in an operation based list, that the list resolve the operation again.
     * <p/>
     * Calling this method has no effect on non operation based lists.
     */
    public void clearResolvedReferences();

    /**
     * @return true is the limit defined by calling setMaxObjectsToRetrieve was reached.
     */
    public boolean reachedMaxObjectsToRetrieve();

    public MithraList getNonPersistentGenericCopy();

    public void setNumberOfParallelThreads(int numberOfThreads);

    public boolean notEmpty();

    /**
     *
     * @return null if nothing has been deep fetched, otherwise, the root of the deep fetch tree
     */
    public DeepFetchTree getDeepFetchTree();

    /**
     * if this is an operation based list, copy it to a non-operation based list, retaining the
     * deep fetches.
     * If this is already an adhoc list (non-operation based), just return itself.
     * @return an adhoc list.
     */
    public MithraList<E> asAdhoc();
}
