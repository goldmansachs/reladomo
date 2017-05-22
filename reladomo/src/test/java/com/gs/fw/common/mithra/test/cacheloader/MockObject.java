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

package com.gs.fw.common.mithra.test.cacheloader;

import com.gs.fw.common.mithra.MithraDataObject;
import com.gs.fw.common.mithra.MithraObject;
import com.gs.fw.common.mithra.MithraObjectPortal;
import com.gs.fw.common.mithra.cache.Cache;
import com.gs.fw.common.mithra.cache.offheap.MithraOffHeapDataObject;

import java.io.*;


class MockObject implements MithraObject, MithraDataObject
{
    int nonUniqueKey;
    int identity;

    MockObject(int nonUniqueKey, int identity)
    {
        this.nonUniqueKey = nonUniqueKey;
        this.identity = identity;
    }

    public boolean equals(Object o)
    {
        return this.identity == ((MockObject) o).identity;
    }

    public int hashCode()
    {
        return identity;
    }


    @Override
    public void zReindexAndSetDataIfChanged(MithraDataObject data, Cache cache)
    {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public void zSerializeFullData(ObjectOutput out) throws IOException
    {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public void zWriteDataClassName(ObjectOutput out) throws IOException
    {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public void zSerializeFullTxData(ObjectOutput out) throws IOException
    {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public void zSetData(MithraDataObject data, Object optional)
    {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public MithraDataObject zGetCurrentData()
    {
        return this;
    }

    @Override
    public void zMarkDirty()
    {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public void zSetNonTxPersistenceState(int state)
    {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public boolean isDeletedOrMarkForDeletion()
    {
        return false;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public MithraObject getNonPersistentCopy()
    {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public boolean changed(MithraDataObject newData)
    {
        return false;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public void copyNonPkAttributes(MithraDataObject newData)
    {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public MithraDataObject copy()
    {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public MithraDataObject copy(boolean copyRelationships)
    {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public void zSetDataVersion(byte version)
    {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public byte zGetDataVersion()
    {
        return 0;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public void zIncrementDataVersion()
    {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public void zDeserializeFullData(ObjectInput in) throws IOException, ClassNotFoundException
    {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public String zReadDataClassName(ObjectInput in) throws IOException, ClassNotFoundException
    {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public boolean zAsOfAttributesFromEquals(MithraDataObject other)
    {
        return false;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public boolean zAsOfAttributesChanged(MithraDataObject other)
    {
        return false;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public boolean zNonPrimaryKeyAttributesChanged(MithraDataObject newData)
    {
        return false;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public boolean zNonPrimaryKeyAttributesChanged(MithraDataObject newData, double tolerance)
    {
        return false;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public void zSerializePrimaryKey(ObjectOutput out) throws IOException
    {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public void zDeserializePrimaryKey(ObjectInput in) throws IOException, ClassNotFoundException
    {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public MithraObjectPortal zGetMithraObjectPortal()
    {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public MithraObjectPortal zGetMithraObjectPortal(int hierarchyDepth)
    {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public String zGetPrintablePrimaryKey()
    {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public boolean zHasIdentity()
    {
        return false;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public Number zGetIdentityValue()
    {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public void zSetIdentity(Number identityValue)
    {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public boolean hasSamePrimaryKeyIgnoringAsOfAttributes(MithraDataObject mithraDataObject)
    {
        return false;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public void clearRelationships()
    {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public boolean zHasSameNullPrimaryKeyAttributes(MithraDataObject mithraDataObject)
    {
        return false;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public void clearAllDirectRefs()
    {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public void zSerializeRelationships(ObjectOutputStream out) throws IOException
    {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public void zDeserializeRelationships(ObjectInputStream in) throws IOException, ClassNotFoundException
    {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public MithraOffHeapDataObject zCopyOffHeap()
    {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public String zGetSerializationClassName()
    {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public MithraObjectPortal zGetPortal()
    {
        return null;
    }
}
