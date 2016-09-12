
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

package com.gs.fw.common.mithra.list;

import com.gs.fw.common.mithra.MithraList;
import com.gs.fw.common.mithra.cache.Index;
import com.gs.fw.common.mithra.finder.AbstractRelatedFinder;
import com.gs.fw.common.mithra.finder.orderby.OrderBy;
import com.gs.fw.common.mithra.notification.listener.MithraApplicationNotificationListener;
import com.gs.fw.common.mithra.util.DoWhileProcedure;
import com.gs.fw.common.mithra.util.Filter;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;

public interface MithraDelegatedList<E> extends Serializable
{

    public void sortWith(DelegatingList<E> delegatingList, OrderBy orderBy);

    public boolean isOperationBased();

    public void forceResolve(DelegatingList<E> delegatingList);

    public void forceRefresh(DelegatingList<E> delegatingList);

    public boolean isStale(DelegatingList<E> delegatingList);

    public int count(DelegatingList<E> delegatingList);

    public MithraDelegatedList<E> setMaxObjectsToRetrieve(DelegatingList<E> delegatingList, int count);

    public boolean reachedMaxObjectsToRetrieve(DelegatingList<E> delegatingList);

    public MithraDelegatedList<E> registerForNotification(DelegatingList<E> delegatingList, MithraApplicationNotificationListener listener);

    public Index getInternalIndex(DelegatingList<E> delegatingList);

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
     * to using this operation (use iterator() if deepfetch is needed).
     *
     *
     *
     * @param delegatingList
     * @param closure The code that will be executed for each element of the list.
     */
    public void forEachWithCursor(DelegatingList<E> delegatingList, DoWhileProcedure closure);

    public void forEachWithCursor(DelegatingList<E> delegatingList, DoWhileProcedure closure, Filter filter);

    /**
     * Clears the list of resolved references.</p>
     * This method will clear its internal list of results making all the referenced objects
     * eligible for garbage collection.
     * <p/>
     * Be aware that calling any method on a list (i.e. size()) after calling this method will cause,
     * in an operation based list, that the list resolve the operation again.
     * <p/>
     * Calling this method has no effect on non operation based lists.
     * @param delegatingList
     */
    public void clearResolvedReferences(DelegatingList<E> delegatingList);

    public boolean isModifiedSinceDetachment(DelegatingList<E> delegatingList);

    public void incrementalDeepFetch(DelegatingList<E> delegatingList);

    public MithraList resolveRelationship(DelegatingList<E> delegatingList, AbstractRelatedFinder finder);

    public MithraList zCloneForRelationship(DelegatingList<E> delegatingList);

    public MithraDelegatedList<E> setNumberOfParallelThreads(DelegatingList<E> delegatingList, int numberOfThreads);

    public int getNumberOfParallelThreads();

    public void zMarkMoved(DelegatingList<E> delegatingList, E item);

    public MithraDelegatedList<E> setForceImplicitJoin(DelegatingList<E> delegatingList, boolean forceImplicitJoin);

    public void init(DelegatingList<E> delegatingList, int initialSize);

    public void init(DelegatingList<E> delegatingList, Collection c);

    public MithraDelegatedList<E> prepareForCount();

    public MithraDelegatedList<E> getNonPersistentDelegate();

    public void writeObject(DelegatingList<E> delegatingList, ObjectOutputStream out) throws IOException;

    public void readObject(DelegatingList<E> delegatingList, ObjectInputStream in) throws IOException, ClassNotFoundException;

    // methods from List
    public int size(DelegatingList<E> delegatingList);
    public boolean contains(DelegatingList<E> delegatingList, Object o);
    public Object[] toArray(DelegatingList<E> delegatingList);
    public <T> T[] toArray(DelegatingList<E> delegatingList, T[] a);
    public boolean add(DelegatingList<E> delegatingList, E e);
    public boolean remove(DelegatingList<E> delegatingList, Object o);
    public boolean containsAll(DelegatingList<E> delegatingList, Collection<?> c);
    public boolean addAll(DelegatingList<E> delegatingList, Collection<? extends E> c);
    public boolean addAll(DelegatingList<E> delegatingList, int index, Collection<? extends E> c);
    public boolean removeAll(DelegatingList<E> delegatingList, Collection<?> c);
    public boolean retainAll(DelegatingList<E> delegatingList, Collection<?> c);
    public void clear(DelegatingList<E> delegatingList);
    public boolean equals(DelegatingList<E> delegatingList, Object o);
    public int hashCode(DelegatingList<E> delegatingList);
    public E get(DelegatingList<E> delegatingList, int index);
    public E set(DelegatingList<E> delegatingList, int index, E element);
    public void add(DelegatingList<E> delegatingList, int index, E element);
    public E remove(DelegatingList<E> delegatingList, int index);
    public int indexOf(DelegatingList<E> delegatingList, Object o);
    public int lastIndexOf(DelegatingList<E> delegatingList, Object o);
    public ListIterator<E> listIterator(DelegatingList<E> delegatingList);
    public ListIterator<E> listIterator(DelegatingList<E> delegatingList, int index);
    public List<E> subList(DelegatingList<E> delegatingList, int fromIndex, int toIndex);

    public Iterator<E> iterator(DelegatingList<E> delegatingList);
}
