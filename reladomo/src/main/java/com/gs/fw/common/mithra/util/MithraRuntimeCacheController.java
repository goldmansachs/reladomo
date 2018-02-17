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

package com.gs.fw.common.mithra.util;

import com.gs.fw.common.mithra.MithraBusinessException;
import com.gs.fw.common.mithra.MithraDataObject;
import com.gs.fw.common.mithra.MithraException;
import com.gs.fw.common.mithra.MithraObject;
import com.gs.fw.common.mithra.MithraObjectDeserializer;
import com.gs.fw.common.mithra.MithraObjectPortal;
import com.gs.fw.common.mithra.cache.Cache;
import com.gs.fw.common.mithra.cache.PrimaryKeyIndex;
import com.gs.fw.common.mithra.database.MithraCodeGeneratedDatabaseObject;
import com.gs.fw.common.mithra.finder.Operation;
import com.gs.fw.common.mithra.finder.RelatedFinder;
import com.gs.fw.common.mithra.util.lz4.LZ4BlockInputStream;
import com.gs.fw.common.mithra.util.lz4.LZ4BlockOutputStream;
import com.gs.reladomo.metadata.ReladomoClassMetaData;
import org.eclipse.collections.impl.list.mutable.FastList;
import org.eclipse.collections.impl.set.mutable.UnifiedSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.lang.reflect.Method;
import java.util.List;
import java.util.Set;



public class MithraRuntimeCacheController
{
    private static Logger logger = LoggerFactory.getLogger(MithraRuntimeCacheController.class.getName());

    public static final byte CACHE_ARCHIVE_VERSION = 2;
    public static final byte JZLIB_CACHE_COMPRESSION_VERSION = 1;
    public static final byte CACHE_COMPRESSION_VERSION = 2;

    public static final int POST_COMPRESS_BUFFER_SIZE = 8092;
    public static final int PRE_COMPRESS_BUFFER_SIZE = POST_COMPRESS_BUFFER_SIZE * 4;

    private ReladomoClassMetaData metaData;
    private Class finderClass;
    private RelatedFinder relatedFinder;
    private static final Object[] NULL_ARGS = (Object[]) null;

    public MithraRuntimeCacheController(Class finderClass)
    {
        this.metaData = ReladomoClassMetaData.fromFinderClass(finderClass);
        this.finderClass = finderClass;
        this.relatedFinder = this.metaData.getFinderInstance();
    }

    public ReladomoClassMetaData getMetaData()
    {
        return metaData;
    }

    protected Object invokeStaticMethod(Class classToInvoke, String methodName)
    {
        try
        {
            Method method = ReflectionMethodCache.getZeroArgMethod(classToInvoke, methodName);
            return method.invoke(null, NULL_ARGS);
        }
        catch (Exception e)
        {
            logger.error("Could not invoke method "+methodName+" on class "+classToInvoke, e);
            throw new MithraException("Could not invoke method "+methodName+" on class "+classToInvoke, e);
        }
    }

    public Class getFinderClass()
    {
        return this.metaData.getFinderClass();
    }

    public String getClassName()
    {
        return metaData.getBusinessOrInterfaceClassName();
    }

    public void clearQueryCache()
    {
        if (this.relatedFinder.isTemporary())
        {
            invokeStaticMethod(getFinderClass(), "clearQueryCache");
        }
        else
        {
            this.getMithraObjectPortal().clearQueryCache();
        }
    }

    public void reloadCache()
    {
        if (!this.relatedFinder.isTemporary())
        {
            this.getMithraObjectPortal().reloadCache();
        }
    }

    public void clearPartialCacheOrReloadFullCache()
    {
        if (this.getCache().isPartialCache())
        {
            this.clearQueryCache();
        }
        else
        {
            this.reloadCache();
        }
    }

    public MithraObjectPortal getMithraObjectPortal()
    {
        return this.relatedFinder.getMithraObjectPortal();
    }

    protected Cache getCache()
    {
        return this.getMithraObjectPortal().getCache();
    }

    public RelatedFinder getFinderInstanceFromFinderClass()
    {
        return this.metaData.getFinderInstance();
    }

    public RelatedFinder getFinderInstance()
    {
        return this.relatedFinder;
    }

    public boolean isPartialCache()
    {
        return this.relatedFinder.isTemporary() || this.getCache().isPartialCache();
    }

    public int getCacheSize()
    {
        if (this.relatedFinder.isTemporary()) return 0;
        return this.getCache().size();
    }

    public boolean equals(Object o)
    {
        if (this == o)
        {
            return true;
        }
        if (!(o instanceof MithraRuntimeCacheController))
        {
            return false;
        }

        final MithraRuntimeCacheController mithraRuntimeCacheController = (MithraRuntimeCacheController) o;
        return finderClass.equals(mithraRuntimeCacheController.finderClass);
    }

    public int hashCode()
    {
        return finderClass.hashCode();
    }

    public void archiveObjects(OutputStream out, List<? extends MithraObject> objects) throws IOException
    {
        MithraArchiveWriter writer = getMithraArchiveWriter(out);
        try
        {
            writer.archiveObjectsIncrementally(objects);
        }
        finally
        {
            writer.finish();
        }
    }

    public MithraArchiveWriter getMithraArchiveWriter(OutputStream out)
    {
        return new MithraArchiveWriter(this, out);
    }

    public void archiveCache(OutputStream out) throws IOException
    {
        archiveCache(out, null);
    }

    public void archiveCache(OutputStream out, Filter filterDatesToKeep) throws IOException
    {
        MithraObjectPortal portal = getMithraObjectPortal();
        Cache cache = portal.getCache();
        LZ4BlockOutputStream zip = null;
        try
        {
            out = new BufferedOutputStream(out, POST_COMPRESS_BUFFER_SIZE);
            out.write(CACHE_ARCHIVE_VERSION);
            out.write(CACHE_COMPRESSION_VERSION);
            zip = new LZ4BlockOutputStream(out, false);
            ObjectOutputStream oos = new ObjectOutputStream(zip);
            oos.writeObject(getClassName());
            oos.writeInt(portal.getFinder().getSerialVersionId());
            if (filterDatesToKeep == null)
            {
                cache.archiveCache(oos);
            }
            else
            {
                cache.archiveCacheWithFilter(oos, filterDatesToKeep);
            }
            oos.writeInt(0);
            oos.flush();
            zip.finish();
            out.flush();
            zip = null;
        }
        finally
        {
            if (zip != null)
            {
                zip.finish();
                zip.close();
            }
        }
    }

    public void readCacheFromArchive(InputStream in) throws IOException, ClassNotFoundException
    {
        readArchive(in, true, null);
    }

    public void readCacheFromArchiveDoNotRemoveLeftOver(InputStream in) throws IOException, ClassNotFoundException
    {
        readArchive(in, false, null);
    }

    public void readCacheFromArchiveDoNotRemoveLeftOver(InputStream in, Filter filterOfDatesToKeep) throws IOException, ClassNotFoundException
    {
        readArchive(in, false, filterOfDatesToKeep);
    }

    private void readArchive(InputStream in, boolean removeLeftOver, Filter filterOfDatesToKeep)
            throws IOException, ClassNotFoundException
    {
        in = new BufferedInputStream(in, POST_COMPRESS_BUFFER_SIZE);
        MithraObjectPortal portal = this.getMithraObjectPortal();
        Cache cache = portal.getCache();
        PrimaryKeyIndex fullUniqueIndex = null;
        if (!this.isPartialCache())
        {
            fullUniqueIndex = cache.getPrimayKeyIndexCopy();
        }
        MithraObjectDeserializer deserializer = portal.getMithraObjectDeserializer();
        FastList newDataList = new FastList();
        FastList updatedDataList = new FastList();
        byte archiveVersion = (byte) in.read();
        if (archiveVersion > CACHE_ARCHIVE_VERSION)
        {
            throw new MithraBusinessException("unknown cache archive version "+archiveVersion);
        }
        ObjectInputStream ois = null;
        byte compression = (byte) in.read();
        if (compression == JZLIB_CACHE_COMPRESSION_VERSION)
        {
            throw new MithraBusinessException("Jzlib compression is no longer supported. Use an older version of Mithra (14.6.x is the last version with Jzlib support)");
        }
        else if (compression == CACHE_COMPRESSION_VERSION)
        {
            ois = new ObjectInputStream(new LZ4BlockInputStream(in));
        }
        if (compression > CACHE_COMPRESSION_VERSION)
        {
            throw new MithraBusinessException("unknown cache compression type "+compression);
        }
        String finderClassName = (String) ois.readObject();
        if (!finderClassName.equals(getClassName()))
        {
            throw new MithraBusinessException("Wrong cache archive. Expecting "+getClassName()+" but got "+
             finderClassName);
        }
        int serialVersion = ois.readInt();
        if (serialVersion != portal.getFinder().getSerialVersionId())
        {
            throw new MithraBusinessException("Wrong serial version for class "+getClassName()+" Expecting "+
                portal.getFinder().getSerialVersionId()+" but got "+serialVersion);
        }
        boolean waitForZero = archiveVersion > 1;
        boolean done;
        Set<String> databaseIdentifiers = UnifiedSet.newSet(4);

        int chunkSize = removeLeftOver ? Integer.MAX_VALUE : 500000; // cannot chunk if need to removeLeftovers
        do
        {
            int size = ois.readInt();
            done = size == 0;

            if (portal.isPureHome())
            {
                for(int i=0;i<size;i++)
                {
                    MithraDataObject data = deserializer.deserializeFullData(ois);
                    //check if this object is from the right date. If not, don't add to any list
                    if(filterOfDatesToKeep == null || !filterOfDatesToKeep.matches(data))
                    {
                        deserializer.analyzeChangeForReload(fullUniqueIndex, data, newDataList, updatedDataList);
                    }

                    if (newDataList.size() + updatedDataList.size() > chunkSize)
                    {
                        cache.updateCache(newDataList, updatedDataList, ListFactory.EMPTY_LIST); // only called with removeLeftOver == false
                        newDataList.clear();
                        updatedDataList.clear();
                    }
                }
            }
            else
            {
                MithraCodeGeneratedDatabaseObject databaseObject = (MithraCodeGeneratedDatabaseObject) this.getMithraObjectPortal().getDatabaseObject();
                UnifiedSet sources = UnifiedSet.newSet();
                for(int i=0;i<size;i++)
                {
                    MithraDataObject data = deserializer.deserializeFullData(ois);
                    //check if this object is from the right date. If not, don't add to any list
                    if(filterOfDatesToKeep == null || !filterOfDatesToKeep.matches(data))
                    {
                        Object sourceAttribute = databaseObject.getSourceAttributeValueFromObjectGeneric(data);
                        sources.add(sourceAttribute);
                        deserializer.analyzeChangeForReload(fullUniqueIndex, data, newDataList, updatedDataList);
                    }

                    if (newDataList.size() + updatedDataList.size() > chunkSize)
                    {
                        cache.updateCache(newDataList, updatedDataList, ListFactory.EMPTY_LIST); // only called with removeLeftOver == false
                        newDataList.clear();
                        updatedDataList.clear();
                    }
                }
                for (Object each : sources)
                {
                    databaseIdentifiers.add(databaseObject.getDatabaseIdentifierGenericSource(each));
                }
            }
        }
        while(waitForZero && !done);
        List leftOver = ListFactory.EMPTY_LIST;
        if (removeLeftOver)
        {
            if (this.isPartialCache())
            {
                this.clearQueryCache();
            }
            else
            {
                leftOver = fullUniqueIndex.getAll();
            }
        }
        cache.updateCache(newDataList, updatedDataList, leftOver);
        this.getMithraObjectPortal().incrementClassUpdateCount();
        for (String dbId : databaseIdentifiers)
        {
            this.getMithraObjectPortal().registerForNotification(dbId);
        }
    }

    public boolean isTemporaryObject()
    {
        return this.relatedFinder.isTemporary();
    }

    public long getOffHeapAllocatedDataSize()
    {
        return this.getCache().getOffHeapAllocatedDataSize();
    }

    public long getOffHeapUsedDataSize()
    {
        return this.getCache().getOffHeapUsedDataSize();
    }

    public long getOffHeapAllocatedIndexSize()
    {
        return this.getCache().getOffHeapAllocatedIndexSize();
    }

    public long getOffHeapUsedIndexSize()
    {
        return this.getCache().getOffHeapUsedIndexSize();
    }

    public RenewedCacheStats renewCacheForOperation(Operation op)
    {
        RenewedCacheStats stats =  this.getMithraObjectPortal().renewCacheForOperation(op);
        this.clearQueryCache();
        return stats;
    }
}
