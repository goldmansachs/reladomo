
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

import com.gs.fw.common.mithra.MithraBusinessException;
import com.gs.fw.common.mithra.MithraList;
import com.gs.fw.common.mithra.MithraObjectPortal;
import com.gs.fw.common.mithra.MithraTransactionalList;
import com.gs.fw.common.mithra.attribute.Attribute;
import com.gs.fw.common.mithra.cache.FullUniqueIndex;
import com.gs.fw.common.mithra.cache.Index;
import com.gs.fw.common.mithra.finder.AbstractRelatedFinder;
import com.gs.fw.common.mithra.finder.DeepFetchNode;
import com.gs.fw.common.mithra.finder.Operation;
import com.gs.fw.common.mithra.finder.RelatedFinder;
import com.gs.fw.common.mithra.finder.orderby.OrderBy;
import com.gs.fw.common.mithra.list.cursor.Cursor;
import com.gs.fw.common.mithra.list.merge.TopLevelMergeOptions;
import com.gs.fw.common.mithra.notification.MithraDatabaseIdentifierExtractor;
import com.gs.fw.common.mithra.notification.listener.MithraApplicationNotificationListener;
import com.gs.fw.common.mithra.querycache.CachedQuery;
import com.gs.fw.common.mithra.util.DoWhileProcedure;
import com.gs.fw.common.mithra.util.Filter;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.*;

public class AbstractOperationBasedList<E> implements MithraDelegatedList<E>, Serializable
{
    public static final AbstractOperationBasedList DEFAULT = new AbstractOperationBasedList(true);

    private int maxObjectsToRetrieve = 0;
    private boolean forceImplicitJoin = false;
    private transient Index pkIndex;
    private int numberOfParallelThreads = 1;
    private boolean isDefault = false;
    private int resolvedSize = -1;

    public AbstractOperationBasedList()
    {
    }

    protected AbstractOperationBasedList newCopy()
    {
        return new AbstractOperationBasedList();
    }

    protected AbstractOperationBasedList(boolean forDefaultUnused)
    {
        this.isDefault = true;
    }

    public MithraDelegatedList setForceImplicitJoin(DelegatingList delegatingList, boolean forceImplicitJoin)
    {
        AbstractOperationBasedList result = copyIfDefault();
        result.forceImplicitJoin = forceImplicitJoin;
        return result;
    }

    protected AbstractOperationBasedList copyIfDefault()
    {
        return isDefault ? newCopy() : this;
    }

    public boolean isForceImplicitJoin()
    {
        return forceImplicitJoin;
    }

    @Override
    public void init(DelegatingList<E> es, Collection c)
    {
        throw new RuntimeException("should never get here");
    }

    @Override
    public MithraDelegatedList<E> prepareForCount()
    {
        return this.copyIfDefault();
    }

    @Override
    public MithraDelegatedList<E> getNonPersistentDelegate()
    {
        return AbstractNonOperationBasedList.DEFAULT;
    }

    @Override
    public void writeObject(DelegatingList<E> delegatingList, ObjectOutputStream out) throws IOException
    {
        // do nothing. we'll re-resolve on load
    }

    @Override
    public void readObject(DelegatingList<E> delegatingList, ObjectInputStream in) throws IOException
    {
        //nothing to do
    }

    @Override
    public void init(DelegatingList<E> es, int initialSize)
    {
        throw new RuntimeException("should never get here");
    }

    protected CachedQuery getResolved(DelegatingList delegatingList)
    {
        return (CachedQuery) delegatingList.zGetFastListOrCachedQuery();
    }

    protected void clearResolved(DelegatingList delegatingList)
    {
        synchronized(delegatingList)
        {
            delegatingList.zSetFastListOrCachedQuery(null);
            this.resolvedSize = -1; //this is safe for DEFAULT
            DeepFetchNode root = delegatingList.getDeepFetchedRelationships();
            if (root != null)
            {
                root.clearResolved();
            }
        }
    }

    public MithraDelegatedList setNumberOfParallelThreads(DelegatingList delegatingList, int numberOfThreads)
    {
        AbstractOperationBasedList result = copyIfDefault();
        result.numberOfParallelThreads = numberOfThreads;
        return result;
    }

    public int getNumberOfParallelThreads()
    {
        return this.numberOfParallelThreads;
    }

    public MithraDelegatedList setMaxObjectsToRetrieve(DelegatingList delegatingList, int maxObjectsToRetrieve)
    {
        AbstractOperationBasedList result = copyIfDefault();
        result.maxObjectsToRetrieve = maxObjectsToRetrieve;
        return result;
    }

    public boolean reachedMaxObjectsToRetrieve(DelegatingList delegatingList)
    {
        this.resolveOperation(delegatingList);
        return this.getResolved(delegatingList).reachedMaxRetrieveCount();
    }

    public int count(DelegatingList delegatingList)
    {
        int result = this.getResolvedSize(delegatingList);
        if (result < 0)
        {
            MithraObjectPortal resultObjectPortal = delegatingList.getOperation().getResultObjectPortal();
            result = resultObjectPortal.count(delegatingList.getOperation());
            this.resolvedSize = result;
        }
        return result;
    }

    public boolean isStale(DelegatingList delegatingList)
    {
        if (this.isOperationResolved(delegatingList))
        {
            return getResolved(delegatingList).isExpired();
        }
        return false;
    }

    @Override
    public CachedQuery getCachedQuery(DelegatingList<E> delegatingList)
    {
        if (this.isOperationResolved(delegatingList))
        {
            return getResolved(delegatingList);
        }
        return null;
    }

    protected boolean isOperationResolved(DelegatingList<E> delegatingList)
    {
        return delegatingList.zGetFastListOrCachedQuery() != null;
    }

    protected List resolveOperation(DelegatingList delegatingList)
    {
        return resolveOperation(delegatingList, delegatingList.isBypassCache());
    }

    protected List resolveOperation(DelegatingList delegatingList, boolean bypassCache)
    {
        long queryTime = 0;
        if (!this.isOperationResolved(delegatingList))
        {
            synchronized (delegatingList)
            {
                if (!this.isOperationResolved(delegatingList)) // re-do under synchronized lock
                {
                    queryTime = System.currentTimeMillis();
                    Operation originalOp = delegatingList.getOperation();
                    CachedQuery resolved = originalOp.getResultObjectPortal().findAsCachedQuery(originalOp,
                            delegatingList.getOrderBy(), bypassCache, delegatingList.zIsForRelationship(), this.maxObjectsToRetrieve, this.getNumberOfParallelThreads(), this.forceImplicitJoin);
                    delegatingList.zSetFastListOrCachedQuery(resolved);
                    if (originalOp != resolved.getOperation())
                    {
                        delegatingList.zSetOperation(resolved.getOperation());
                    }
                    queryTime = System.currentTimeMillis() - queryTime;
                }
            }
        }
        this.resolveDeepRelationships(delegatingList, queryTime);
        return ((CachedQuery)delegatingList.zGetFastListOrCachedQuery()).getResult();
    }

    private Cursor createCursor(DelegatingList delegatingList, Filter postLoadList)
    {
        synchronized (delegatingList)
        {
            return delegatingList.getOperation().getResultObjectPortal().findCursorFromServer(delegatingList.getOperation(),
                    postLoadList, delegatingList.getOrderBy(), this.maxObjectsToRetrieve, delegatingList.isBypassCache(),
                    this.getNumberOfParallelThreads(), this.forceImplicitJoin);
        }
    }

    public void forceRefresh(DelegatingList delegatingList)
    {
        synchronized (delegatingList)
        {
            delegatingList.zSetFastListOrCachedQuery(null);
            this.resolveOperation(delegatingList, true);
        }
    }

    protected List resolveOperationInMemory(DelegatingList<E> delegatingList)
    {
        synchronized (delegatingList)
        {
            if (!delegatingList.isBypassCache())
            {
                if (!this.isOperationResolved(delegatingList))
                {
                    Operation originalOp = delegatingList.getOperation();
                    MithraObjectPortal portal = originalOp.getResultObjectPortal();
                    if (!portal.isCacheDisabled())
                    {
                        CachedQuery resolved = portal.zFindInMemory(originalOp, delegatingList.getOrderBy());
                        delegatingList.zSetFastListOrCachedQuery(resolved);
                        if (resolved != null && originalOp != resolved.getOperation())
                        {
                            delegatingList.zSetOperation(resolved.getOperation());
                        }
                    }
                }
                if (this.isOperationResolved(delegatingList))
                {
                    return this.getResolved(delegatingList).getResult();
                }
            }
        }
        return null;
    }

    protected int getResolvedSize(DelegatingList delegatingList)
    {
        if (this.isOperationResolved(delegatingList))
        {
            return this.getResolved(delegatingList).getResult().size();
        }
        if (resolvedSize < 0)
        {
            List resolved = this.resolveOperationInMemory(delegatingList);
            if (resolved != null)
            {
                return resolved.size();
            }
        }
        return this.resolvedSize;
    }

    public void sortWith(DelegatingList delegatingList, OrderBy orderBy)
    {
        if (this.isOperationResolved(delegatingList))
        {
            delegatingList.zSetFastListOrCachedQuery(this.getResolved(delegatingList).getCloneIfDifferentOrderBy(orderBy));
        }
    }

    public boolean isOperationBased()
    {
        return true;
    }

    public void forceResolve(DelegatingList delegatingList)
    {
        this.resolveOperation(delegatingList);
    }

    public MithraDelegatedList registerForNotification(DelegatingList delegatingList, MithraApplicationNotificationListener listener)
    {
        AbstractOperationBasedList result = copyIfDefault();
        Operation op = delegatingList.getOperation();
        Map databaseIdentifierMap = delegatingList.getMithraObjectPortal().extractDatabaseIdentifiers(op);
        Set keySet = databaseIdentifierMap.keySet();

        if (result.pkIndex == null)
        {
            Attribute[] primaryKeyAttributes = delegatingList.getMithraObjectPortal().getFinder().getPrimaryKeyAttributes();
            result.pkIndex = new FullUniqueIndex("OperationBasedListIndex", primaryKeyAttributes);
            int size = this.size(delegatingList);
            for (int i = 0; i < size; i++)
            {
                result.pkIndex.put(this.get(delegatingList, i));
            }
        }

        for (Iterator it = keySet.iterator(); it.hasNext();)
        {
            MithraDatabaseIdentifierExtractor.DatabaseIdentifierKey key =
                    (MithraDatabaseIdentifierExtractor.DatabaseIdentifierKey) it.next();
            RelatedFinder finder = key.getFinder();
            MithraObjectPortal portal = finder.getMithraObjectPortal();
            if (portal == delegatingList.getOperation().getResultObjectPortal())
            {
                portal.registerForApplicationNotification((String) databaseIdentifierMap.get(key), listener, delegatingList, op);
            }
        }
        return result;
    }

    public Index getInternalIndex(DelegatingList<E> delegatingList)
    {
        return this.pkIndex;
    }

    protected void resolveDeepRelationships(DelegatingList delegatingList, long queryTime)
    {
        DeepFetchNode root = delegatingList.getDeepFetchedRelationships();
        if (root != null)
        {
            root.deepFetch(this.getResolved(delegatingList), delegatingList.isBypassCache(), queryTime, numberOfParallelThreads, this.forceImplicitJoin);
        }
    }

    public void incrementalDeepFetch(DelegatingList delegatingList)
    {
        synchronized (delegatingList)
        {
            if (this.getResolved(delegatingList) != null)
            {
                delegatingList.getDeepFetchedRelationships().incrementalDeepFetch(this.getResolved(delegatingList), delegatingList.isBypassCache(), this.forceImplicitJoin);
            }
        }
    }

    public void forEachWithCursor(DelegatingList delegatingList, DoWhileProcedure closure)
    {
        this.forEachWithCursor(delegatingList, closure, null);
    }

    public void forEachWithCursor(DelegatingList delegatingList, DoWhileProcedure closure, Filter postLoadFilter)
    {
        List resolvedResult;
        if ((resolvedResult = this.resolveOperationInMemory(delegatingList)) != null)
        {
            // The operation can be resolved in memory (Full or Partial Cache).
            int resultSize = resolvedResult.size();
            for (int i = 0; i < resultSize; i++)
            {
                Object each = resolvedResult.get(i);
                if (postLoadFilter == null || postLoadFilter.matches(each))
                {
                    if (!closure.execute(each)) break;
                }
            }
        }
        else
        {
            // The operation can't be resolved, then use the cursor.
            Cursor c = this.createCursor(delegatingList, postLoadFilter);
            try
            {
                while (c.hasNext() && closure.execute(c.next())) ;
            }
            finally
            {
                c.close();
            }
        }
    }

    public void zMarkMoved(DelegatingList delegatingList, Object item)
    {
        throw new RuntimeException("should not get here");
    }

    public void clearResolvedReferences(DelegatingList delegatingList)
    {
        this.clearResolved(delegatingList);
    }

    public boolean isModifiedSinceDetachment(DelegatingList delegatingList)
    {
        return false;
    }

    public MithraList resolveRelationship(DelegatingList<E> delegatingList, AbstractRelatedFinder finder)
    {
        return finder.findManyWithMapper(delegatingList.getOperation());
    }

    public DelegatingList zCloneForRelationship(DelegatingList delegatingList)
    {
        DelegatingList result = (DelegatingList) delegatingList.getOperation().getResultObjectPortal().getFinder().findMany(delegatingList.getOperation());
        result.zSetForRelationship();
        CachedQuery resolved = this.getResolved(delegatingList);
        if (resolved != null)
        {
            if (!resolved.isExpired())
            {
                result.zSetFastListOrCachedQuery(delegatingList.zGetFastListOrCachedQuery());
            }
        }
        return result;
    }

    //list methods
    public boolean equals(DelegatingList<E> delegatingList, Object o)
    {
        return resolveOperation(delegatingList).equals(o);
    }

    public int hashCode(DelegatingList<E> delegatingList)
    {
        return resolveOperation(delegatingList).hashCode();
    }

    public Object[] toArray(DelegatingList<E> delegatingList)
    {
        return resolveOperation(delegatingList).toArray();
    }

    public E get(DelegatingList<E> delegatingList, int index)
    {
        return (E) resolveOperation(delegatingList).get(index);
    }

    public int indexOf(DelegatingList<E> delegatingList, Object o)
    {
        return resolveOperation(delegatingList).indexOf(o);
    }

    public int lastIndexOf(DelegatingList<E> delegatingList, Object o)
    {
        return resolveOperation(delegatingList).lastIndexOf(o);
    }

    public int size(DelegatingList<E> delegatingList)
    {
        return this.resolveOperation(delegatingList).size();
    }

    public boolean contains(DelegatingList<E> delegatingList, Object o)
    {
        return resolveOperation(delegatingList).contains(o);
    }

    public boolean containsAll(DelegatingList<E> delegatingList, Collection<?> c)
    {
        return resolveOperation(delegatingList).containsAll(c);
    }

    public List subList(DelegatingList<E> delegatingList, int fromIndex, int toIndex)
    {
        return Collections.unmodifiableList(resolveOperation(delegatingList).subList(fromIndex, toIndex));
    }

    public <T> T[] toArray(DelegatingList<E> delegatingList, T[] a)
    {
        return (T[]) resolveOperation(delegatingList).toArray(a);
    }

    public void clear(DelegatingList<E> delegatingList)
    {
        throw new MithraBusinessException("List is unmodifiable");
    }

    public E remove(DelegatingList<E> delegatingList, int index)
    {
        throw new MithraBusinessException("an operation based list cannot be changed. Make a copy before removing");
    }

    public boolean remove(DelegatingList<E> delegatingList, Object o)
    {
        throw new MithraBusinessException("an operation based list cannot be changed. Make a copy before removing");
    }

    public void add(DelegatingList<E> delegatingList, int index, E element)
    {
        throw new MithraBusinessException("Can't set a particular index in an operation based list.");
    }

    public boolean add(DelegatingList<E> delegatingList, E o)
    {
        throw new MithraBusinessException("Cannot change an operation based list. Make a copy before adding.");
    }

    public boolean addAll(DelegatingList<E> delegatingList, int index, Collection<? extends E> c)
    {
        throw new MithraBusinessException("List is unmodifiable");
    }

    public boolean addAll(DelegatingList<E> delegatingList, Collection<? extends E> c)
    {
        throw new MithraBusinessException("Can't modify an operation based list.");
    }

    public boolean removeAll(DelegatingList<E> delegatingList, Collection<?> c)
    {
        throw new RuntimeException("List is unmodifiable");
    }

    public boolean retainAll(DelegatingList<E> delegatingList, Collection<?> c)
    {
        throw new RuntimeException("List is unmodifiable");
    }

    public Iterator<E> iterator(DelegatingList<E> delegatingList)
    {
        return Collections.unmodifiableList(resolveOperation(delegatingList)).iterator();
    }

    public ListIterator listIterator(DelegatingList<E> delegatingList)
    {
        //todo: change this to Collections.unmodifiableList; doing so will break calls to Collections.sort
        return resolveOperation(delegatingList).listIterator();
    }

    public ListIterator listIterator(DelegatingList<E> delegatingList, int index)
    {
        return Collections.unmodifiableList(resolveOperation(delegatingList)).listIterator(index);
    }

    public E set(DelegatingList<E> delegatingList, int index, E element)
    {
        throw new MithraBusinessException("Can't modify an operation based list.");
    }

    @Override
    public MithraList<E> asAdhoc(DelegatingList<E> delegatingList)
    {
        return delegatingList.zCopyIntoAdhoc(delegatingList.getOperation().getResultObjectPortal().getFinder().constructEmptyList());
    }

    @Override
    public void merge(DelegatingList<E> adhoc, MithraList<E> incoming, TopLevelMergeOptions<E> mergeOptions)
    {
        throw new RuntimeException("Should never get here");
    }
}
