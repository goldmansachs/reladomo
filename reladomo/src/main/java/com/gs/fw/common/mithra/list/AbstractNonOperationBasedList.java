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
import com.gs.fw.common.mithra.MithraObjectPortal;
import com.gs.fw.common.mithra.MithraTransactionalList;
import com.gs.fw.common.mithra.cache.Index;
import com.gs.fw.common.mithra.finder.AbstractRelatedFinder;
import com.gs.fw.common.mithra.finder.DeepFetchNode;
import com.gs.fw.common.mithra.list.merge.TopLevelMergeOptions;
import com.gs.fw.common.mithra.querycache.CachedQuery;
import com.gs.fw.common.mithra.util.DoWhileProcedure;
import com.gs.fw.common.mithra.util.Filter;
import com.gs.fw.common.mithra.finder.orderby.OrderBy;
import com.gs.fw.common.mithra.notification.listener.MithraApplicationNotificationListener;

import java.io.*;
import java.util.*;



public class AbstractNonOperationBasedList<E> implements MithraDelegatedList<E>
{
    public static final AbstractNonOperationBasedList DEFAULT = new AbstractNonOperationBasedList();

    public AbstractNonOperationBasedList()
    {
        // for Externalizable
        super();
    }

    protected AdhocFastList<E> getFastList(DelegatingList<E> delegatingList)
    {
        return (AdhocFastList<E>) delegatingList.zGetFastListOrCachedQuery();
    }

    public void sortWith(DelegatingList delegatingList, OrderBy orderBy)
    {
        this.getFastList(delegatingList).sortThis(orderBy);
    }

    public MithraDelegatedList setNumberOfParallelThreads(DelegatingList delegatingList, int numberOfThreads)
    {
        return this;
    }

    public int getNumberOfParallelThreads()
    {
        return 1;
    }

    public boolean isOperationBased()
    {
        return false;
    }

    public void forceResolve(DelegatingList delegatingList)
    {
        getFastList(delegatingList).forceResolve();
    }

    public boolean isStale(DelegatingList delegatingList)
    {
        return false;
    }

    @Override
    public CachedQuery getCachedQuery(DelegatingList<E> delegatingList)
    {
        return null;
    }

    public int count(DelegatingList delegatingList)
    {
        return this.getFastList(delegatingList).size();
    }

    public MithraDelegatedList setMaxObjectsToRetrieve(DelegatingList delegatingList, int count)
    {
        return this;
    }

    public boolean reachedMaxObjectsToRetrieve(DelegatingList delegatingList)
    {
        return false;
    }

    @Override
    public int size(DelegatingList<E> delegatingList)
    {
        this.forceResolve(delegatingList);
        return this.getFastList(delegatingList).size();
    }

    @Override
    public E get(DelegatingList<E> delegatingList, int index)
    {
        this.forceResolve(delegatingList);
        return this.getFastList(delegatingList).get(index);
    }

    private void deepFetch(DelegatingList<E> delegatingList)
    {
        DeepFetchNode rootNode = delegatingList.getDeepFetchedRelationships();
        rootNode.deepFetchAdhocList(getFastList(delegatingList), delegatingList.isBypassCache());
    }

    @Override
    public E remove(DelegatingList<E> delegatingList, int index)
    {
        return getFastList(delegatingList).remove(index);
    }

    @Override
    public E set(DelegatingList<E> delegatingList, int index, E element)
    {
        return this.getFastList(delegatingList).set(index, element);
    }

    @Override
    public Object[] toArray(DelegatingList<E> delegatingList)
    {
        return this.getFastList(delegatingList).toArray();
    }

    @Override
    public <T> T[] toArray(DelegatingList<E> delegatingList, T[] a)
    {
        return this.getFastList(delegatingList).toArray(a);
    }

    @Override
    public void add(DelegatingList<E> delegatingList, final int index, final E element)
    {
        getFastList(delegatingList).add(index, element);
    }

    @Override
    public boolean add(DelegatingList<E> delegatingList, E obj)
    {
        return this.getFastList(delegatingList).add(obj);
    }

    @Override
    public boolean addAll(DelegatingList<E> delegatingList, int i, Collection<? extends E> c)
    {
        return this.getFastList(delegatingList).addAll(i, c);
    }

    @Override
    public boolean addAll(DelegatingList<E> delegatingList, Collection<? extends E> c)
    {
        return this.getFastList(delegatingList).addAll(c);
    }

    @Override
    public void clear(DelegatingList<E> delegatingList)
    {
        this.getFastList(delegatingList).clear();
    }

    public MithraDelegatedList registerForNotification(DelegatingList delegatingList, MithraApplicationNotificationListener listener)
    {
        this.getFastList(delegatingList).registerForNotification(listener);
        return this;
    }

    public Index getInternalIndex(DelegatingList<E> delegatingList)
    {
        return this.getFastList(delegatingList).getInternalIndex();
    }

    public void forEachWithCursor(DelegatingList delegatingList, DoWhileProcedure closure)
    {
        this.getFastList(delegatingList).forEachWithCursor(closure);
    }

    public void forEachWithCursor(DelegatingList delegatingList, DoWhileProcedure closure, Filter postLoadOperation)
    {
        this.getFastList(delegatingList).forEachWithCursor(closure, postLoadOperation);
    }

    public void zMarkMoved(DelegatingList delegatingList, Object item)
    {
        getFastList(delegatingList).zMarkMoved(item);
    }

    public MithraDelegatedList setForceImplicitJoin(DelegatingList delegatingList, boolean forceImplicitJoin)
    {
        return this;
    }

    public void clearResolvedReferences(DelegatingList delegatingList)
    {
        //does nothing
    }

    public boolean isModifiedSinceDetachment(DelegatingList delegatingList)
    {
        return getFastList(delegatingList).isModifiedSinceDetachment();
    }

    public void forceRefresh(DelegatingList delegatingList)
    {
        this.getFastList(delegatingList).forceRefresh();
    }

    public MithraList resolveRelationship(DelegatingList<E> delegatingList, AbstractRelatedFinder finder)
    {
        return this.getFastList(delegatingList).resolveRelationship(finder);
    }

    public MithraList zCloneForRelationship(DelegatingList delegatingList)
    {
        throw new RuntimeException("should not get here");
    }

    public MithraObjectPortal getMithraObjectPortal(DelegatingList<E> delegatingList)
    {
        return delegatingList.getMithraObjectPortal();
    }

    public void incrementalDeepFetch(DelegatingList delegatingList)
    {
        getFastList(delegatingList).incrementalDeepFetch();
    }

    public void init(DelegatingList<E> delegatingList)
    {
        delegatingList.zSetFastListOrCachedQuery(new AdhocFastList(delegatingList));
    }

    @Override
    public void init(DelegatingList<E> delegatingList, int initialSize)
    {
        delegatingList.zSetFastListOrCachedQuery(new AdhocFastList(delegatingList, initialSize));
    }

    @Override
    public void init(DelegatingList<E> delegatingList, Collection c)
    {
        delegatingList.zSetFastListOrCachedQuery(new AdhocFastList<E>(delegatingList, c));
    }

    @Override
    public MithraDelegatedList<E> prepareForCount()
    {
        return this;
    }

    @Override
    public MithraDelegatedList<E> getNonPersistentDelegate()
    {
        return DEFAULT;
    }

    @Override
    public void writeObject(DelegatingList<E> delegatingList, ObjectOutputStream out) throws IOException
    {
        out.writeObject(getFastList(delegatingList));
    }

    @Override
    public void readObject(DelegatingList<E> delegatingList, ObjectInputStream in) throws IOException, ClassNotFoundException
    {
        delegatingList.zSetFastListOrCachedQuery(in.readObject());
    }
//List methods:

    @Override
    public boolean contains(DelegatingList<E> delegatingList, Object o)
    {
        return this.getFastList(delegatingList).contains(o);
    }

    @Override
    public boolean remove(DelegatingList<E> delegatingList, Object o)
    {
        return this.getFastList(delegatingList).remove(o);
    }

    @Override
    public boolean containsAll(DelegatingList<E> delegatingList, Collection<?> c)
    {
        return this.getFastList(delegatingList).containsAll(delegatingList);
    }

    @Override
    public boolean removeAll(DelegatingList<E> delegatingList, Collection<?> c)
    {
        return this.getFastList(delegatingList).removeAll(c);
    }

    @Override
    public boolean retainAll(DelegatingList<E> delegatingList, Collection<?> c)
    {
        return this.getFastList(delegatingList).retainAll(c);
    }

    @Override
    public boolean equals(DelegatingList<E> delegatingList, Object o)
    {
        return this.getFastList(delegatingList).equals(o);
    }

    @Override
    public int hashCode(DelegatingList<E> delegatingList)
    {
        return this.getFastList(delegatingList).hashCode();
    }

    @Override
    public int indexOf(DelegatingList<E> delegatingList, Object o)
    {
        return this.getFastList(delegatingList).indexOf(o);
    }

    @Override
    public int lastIndexOf(DelegatingList<E> delegatingList, Object o)
    {
        return this.getFastList(delegatingList).lastIndexOf(o);
    }

    @Override
    public ListIterator<E> listIterator(DelegatingList<E> delegatingList)
    {
        return this.getFastList(delegatingList).listIterator();
    }

    @Override
    public ListIterator<E> listIterator(DelegatingList<E> delegatingList, int index)
    {
        return this.getFastList(delegatingList).listIterator(index);
    }

    @Override
    public List<E> subList(DelegatingList<E> delegatingList, int fromIndex, int toIndex)
    {
        return this.getFastList(delegatingList).subList(fromIndex, toIndex);
    }

    @Override
    public Iterator<E> iterator(DelegatingList<E> delegatingList)
    {
        return this.getFastList(delegatingList).iterator();
    }

    @Override
    public MithraList<E> asAdhoc(DelegatingList<E> delegatingList)
    {
        return delegatingList;
    }

    @Override
    public void merge(DelegatingList<E> adhoc, MithraList<E> incoming, TopLevelMergeOptions<E> mergeOptions)
    {
        throw new UnsupportedOperationException("merge is only supported for transactional lists");
    }
}