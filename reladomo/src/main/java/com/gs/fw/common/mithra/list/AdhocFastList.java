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
// Portions copyright Hiroshi Ito. Licensed under Apache 2.0 license

package com.gs.fw.common.mithra.list;


import com.gs.fw.common.mithra.MithraList;
import com.gs.fw.common.mithra.MithraManagerProvider;
import com.gs.fw.common.mithra.MithraObjectPortal;
import com.gs.fw.common.mithra.attribute.Attribute;
import com.gs.fw.common.mithra.cache.ExtractorBasedHashStrategy;
import com.gs.fw.common.mithra.cache.FullUniqueIndex;
import com.gs.fw.common.mithra.cache.Index;
import com.gs.fw.common.mithra.finder.AbstractRelatedFinder;
import com.gs.fw.common.mithra.finder.DeepFetchNode;
import com.gs.fw.common.mithra.finder.orderby.OrderBy;
import com.gs.fw.common.mithra.notification.MithraDatabaseIdentifierExtractor;
import com.gs.fw.common.mithra.notification.listener.MithraApplicationNotificationListener;
import com.gs.fw.common.mithra.util.DoWhileProcedure;
import com.gs.fw.common.mithra.util.Filter;
import org.eclipse.collections.impl.list.mutable.FastList;
import org.eclipse.collections.impl.map.mutable.UnifiedMap;
import org.eclipse.collections.impl.set.mutable.UnifiedSet;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.Set;

public class AdhocFastList<E> extends FastList<E>
{
    private transient Index pkIndex;
    private transient boolean isRegisteredForNotification;
    private boolean bypassCache = false;
    private boolean mustDeepFetch = false;
    protected DelegatingList originalList;

    public AdhocFastList()
    {
        // for Externalizable
        super();
    }

    public AdhocFastList(DelegatingList originalList)
    {
        this.originalList = originalList;
    }

    public AdhocFastList(DelegatingList originalList, int initialCapacity)
    {
        super(initialCapacity);
        this.originalList = originalList;
    }

    public AdhocFastList(DelegatingList originalList, Collection c)
    {
        super(c);
        this.originalList = originalList;
    }

    public void setOrderBy(OrderBy orderBy)
    {
        this.sortThis(orderBy);
    }

    public void forceResolve()
    {
        if (mustDeepFetch) deepFetch();
    }

    public void setBypassCache(boolean bypassCache)
    {
        this.bypassCache = bypassCache;
    }

    public int count()
    {
        return this.size();
    }

    @Override
    public ListIterator<E> listIterator(int index)
    {
        this.forceResolve();
        return super.listIterator(index);
    }

    @Override
    public Iterator<E> iterator()
    {
        this.forceResolve();
        return super.iterator();
    }

    @Override
    public ListIterator<E> listIterator()
    {
        this.forceResolve();
        return super.listIterator();
    }

    @Override
    public int size()
    {
        this.forceResolve();
        return super.size();
    }

    @Override
    public E get(int index)
    {
        this.forceResolve();
        return super.get(index);
    }

    private void deepFetch()
    {
        mustDeepFetch = false;
        DeepFetchNode rootNode = originalList.getDeepFetchedRelationships();
        if (rootNode != null) rootNode.deepFetchAdhocList(this, bypassCache);
    }

    @Override
    public boolean remove(Object object)
    {
        // copy of super to ensure implementation always calls remove(int)
        int index = this.indexOf(object);
        if (index >= 0)
        {
            this.remove(index);
            return true;
        }
        return false;
    }

    @Override
    public E remove(int index)
    {
        markChanged();
        E obj = super.remove(index);
        removeHook(obj);
        return obj;
    }

    protected void removeWithoutHook(int index)
    {
        markChanged();
        super.remove(index);
    }

    protected void removeHook(Object obj)
    {
        if (isRegisteredForNotification)
        {
            pkIndex.remove(obj);
        }
    }

    private void markChanged()
    {
        this.mustDeepFetch = true;
        DeepFetchNode rootNode = originalList.getDeepFetchedRelationships();
        if (rootNode != null) rootNode.clearResolved();
    }

    @Override
    public E set(int index, E element)
    {
        markChanged();
        return super.set(index, element);
    }

    @Override
    public Object[] toArray()
    {
        this.forceResolve();
        return super.toArray();
    }

    @Override
    public <E> E[] toArray(E[] array)
    {
        this.forceResolve();
        return super.toArray(array);
    }

    @Override
    public void add(final int index, final E element)
    {
        if (index > -1 && index < this.size)
        {
            addHook(element);
            this.addAtIndex(index, element);
        }
        else if (index == this.size)
        {
            this.add(element);
        }
        else
        {
            throw new IndexOutOfBoundsException("Index: " + index + " Size: " + this.size);
        }
    }

    private void addAtIndex(final int index, final E element)
    {
        final int oldSize = this.size++;
        if (this.items.length == oldSize)
        {
            final Object[] newItems = new Object[(oldSize * 3) / 2 + 1];
            if (index > 0)
            {
                System.arraycopy(this.items, 0, newItems, 0, index);
            }
            System.arraycopy(this.items, index, newItems, index + 1, oldSize - index);
            this.items = (E[]) newItems;
        }
        else
        {
            System.arraycopy(this.items, index, this.items, index + 1, oldSize - index);
        }
        this.items[index] = (E) element;
    }

    @Override
    public boolean add(E obj)
    {
        markChanged();
        addHook(obj);
        return super.add(obj);
    }

    protected void addHook(Object obj)
    {
        if (isRegisteredForNotification)
        {
            addObjectToIndex(obj);
        }
    }

    @Override
    public boolean addAll(int i, Collection c)
    {
        markChanged();
        if (isRegisteredForNotification)
        {
            Iterator collectionIterator = c.iterator();
            while (collectionIterator.hasNext())
            {
                Object mithraObject = collectionIterator.next();
                addObjectToIndex(mithraObject);
            }
        }
        return super.addAll(i, c);
    }

    @Override
    public boolean addAll(Collection c)
    {
        markChanged();
        if (isRegisteredForNotification)
        {
            Iterator collectionIterator = c.iterator();
            while (collectionIterator.hasNext())
            {
                Object mithraObject = collectionIterator.next();
                addObjectToIndex(mithraObject);
            }
        }
        return super.addAll(c);
    }

    private void addObjectToIndex(Object mithraObject)
    {
        Object sourceAttributeValue =
                this.getSourceAttributeValue(this.getMithraObjectPortal().getFinder().getSourceAttribute(), mithraObject);
        Map sourceAttributeToDatabaseIdentifierMap = (Map) MithraManagerProvider.getMithraManager().
                getNotificationEventManager().
                getMithraListToDatabaseIdentiferMap().
                get(pkIndex);
        String databaseIdentifier = (String) sourceAttributeToDatabaseIdentifierMap.get(sourceAttributeValue);

        if (databaseIdentifier == null)
        {
            Set notificationListeners = (Set) MithraManagerProvider.getMithraManager().
                    getNotificationEventManager().
                    getMithraListToNotificationListenerMap().
                    get(pkIndex);

            Iterator it = notificationListeners.iterator();
            while (it.hasNext())
            {
                Set sourceAttributeValueSet = new UnifiedSet(1);
                sourceAttributeValueSet.add(sourceAttributeValue);
                registerForNotification(this.getMithraObjectPortal(), sourceAttributeValueSet, (MithraApplicationNotificationListener) it.next());
            }
        }
        pkIndex.put(mithraObject);
    }

    @Override
    public void clear()
    {
        markChanged();
        if (isRegisteredForNotification)
        {
            pkIndex.clear();
        }
        super.clear();
    }

    private Object getSourceAttributeValue(Attribute sourceAttribute, Object mithraObject)
    {
        Object sourceAttributeValue = null;
        if (sourceAttribute != null)
        {
            sourceAttributeValue = sourceAttribute.valueOf(mithraObject);
        }
        return sourceAttributeValue;
    }

    public void registerForNotification(MithraApplicationNotificationListener listener)
    {
        MithraObjectPortal portal = originalList.getMithraObjectPortal();
        Set sourceAttributeValueSet = new UnifiedSet(3);
        Attribute sourceAttribute = originalList.getMithraObjectPortal().getFinder().getSourceAttribute();
        Map mithraListToNotificationListenerMap = MithraManagerProvider.getMithraManager().
                getNotificationEventManager().
                getMithraListToNotificationListenerMap();
        Set notificationListeners;

        if (!isRegisteredForNotification)
        {
            //initialize index and maps
            Attribute[] primaryKeyAttributes = portal.getFinder().getPrimaryKeyAttributes();
            pkIndex = new FullUniqueIndex("SimpleListIndex", primaryKeyAttributes);

            Map mithraListToDatabaseIdentifierMap = MithraManagerProvider.getMithraManager().
                    getNotificationEventManager().
                    getMithraListToDatabaseIdentiferMap();
            Map sourceAttributeToDatabaseIdentifierMap = (Map) mithraListToDatabaseIdentifierMap.get(pkIndex);

            if (sourceAttributeToDatabaseIdentifierMap == null)
            {
                sourceAttributeToDatabaseIdentifierMap = new UnifiedMap(3);
                mithraListToDatabaseIdentifierMap.put(pkIndex, sourceAttributeToDatabaseIdentifierMap);
            }

            notificationListeners = (Set) mithraListToNotificationListenerMap.get(pkIndex);
            if (notificationListeners == null)
            {
                notificationListeners = new UnifiedSet(3);
                mithraListToNotificationListenerMap.put(pkIndex, notificationListeners);
            }
            isRegisteredForNotification = true;
        }

        if (sourceAttribute == null)
        {
            sourceAttributeValueSet.add(null);
        }

        //populate index
        for (int i = 0; i < this.size(); i++)
        {
            pkIndex.put(this.get(i));
            if (sourceAttribute != null)
            {
                sourceAttributeValueSet.add(getSourceAttributeValue(sourceAttribute, this.get(i)));
            }
        }

        notificationListeners = (Set) mithraListToNotificationListenerMap.get(pkIndex);
        notificationListeners.add(listener);
        registerForNotification(portal, sourceAttributeValueSet, listener);
    }

    private void registerForNotification(MithraObjectPortal portal, Set sourceAttributeValueSet, MithraApplicationNotificationListener listener)
    {

        Map databaseIdentifierMap = portal.extractDatabaseIdentifiers(sourceAttributeValueSet);
        Set keySet = databaseIdentifierMap.keySet();
        for (Iterator it = keySet.iterator(); it.hasNext();)
        {
            MithraDatabaseIdentifierExtractor.DatabaseIdentifierKey key =
                    (MithraDatabaseIdentifierExtractor.DatabaseIdentifierKey) it.next();

            String subject = (String) databaseIdentifierMap.get(key);
            Object sourceAttributeValue = key.getSourceAttributeValue();
            Map mithraListToDatabaseIdentifierMap = MithraManagerProvider.getMithraManager().
                    getNotificationEventManager().
                    getMithraListToDatabaseIdentiferMap();
            Map sourceAttributeToDatabaseIdentifierMap = (Map) mithraListToDatabaseIdentifierMap.get(pkIndex);
            sourceAttributeToDatabaseIdentifierMap.put(sourceAttributeValue, subject);

            portal = key.getFinder().getMithraObjectPortal();
            portal.registerForApplicationNotification(subject, listener, this.originalList, null);
            portal.registerForNotification(subject);
        }
    }

    public Index getInternalIndex()
    {
        return this.pkIndex;
    }

    public void forEachWithCursor(DoWhileProcedure closure)
    {
        int size = size();
        for (int i = 0; i < size && closure.execute(get(i)); i++) ;
    }

    public void forEachWithCursor(DoWhileProcedure closure, Filter postLoadFilter)
    {
        int size = size();
        for (int i = 0; i < size; i++)
        {
            Object each = get(i);
            if (postLoadFilter.matches(each) && !closure.execute(get(i))) break;
        }
    }

    public void zMarkMoved(Object item)
    {
        this.remove(item);
    }

    public void setForceImplicitJoin(boolean forceImplicitJoin)
    {
        //nothing to do
    }

    public void clearResolvedReferences()
    {
        //does nothing
    }

    public boolean isModifiedSinceDetachment()
    {
        return false;
    }

    public void forceRefresh()
    {
        this.originalList.forceRefreshForSimpleList();
    }

    public MithraList resolveRelationship(AbstractRelatedFinder finder)
    {
        FullUniqueIndex index = new FullUniqueIndex(ExtractorBasedHashStrategy.IDENTITY_HASH_STRATEGY);
        if (finder.isToOne())
        {
            for (int i = 0; i < size(); i++)
            {
                Object o = finder.valueOf(this.get(i));
                if (o != null) index.put(o);
            }
        }
        else
        {
            for (int i = 0; i < size(); i++)
            {
                index.addAll((List) finder.valueOf(this.get(i)));
            }
        }
        MithraList list = finder.constructEmptyList();
        list.addAll(index.getAll());
        OrderBy orderBy = finder.zGetOrderBy();
        if (orderBy != null)
        {
            list.setOrderBy(orderBy);
        }
        return list;
    }

    public MithraList zCloneForRelationship()
    {
        throw new RuntimeException("should not get here");
    }

    public MithraObjectPortal getMithraObjectPortal()
    {
        return this.originalList.getMithraObjectPortal();
    }

    public void incrementalDeepFetch()
    {
        mustDeepFetch = true;
//        DeepFetchNode rootNode = originalList.getDeepFetchedRelationships();
//        rootNode.incrementalDeepFetchAdhocList(this, bypassCache);
    }

    @Override
    public void writeExternal(final ObjectOutput out) throws IOException
    {
        super.writeExternal(out);
        out.writeBoolean(bypassCache);
        out.writeBoolean(mustDeepFetch);
        out.writeObject(originalList);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException
    {
        super.readExternal(in);
        this.bypassCache = in.readBoolean();
        this.mustDeepFetch = in.readBoolean();
        this.originalList = (DelegatingList) in.readObject();
    }
}