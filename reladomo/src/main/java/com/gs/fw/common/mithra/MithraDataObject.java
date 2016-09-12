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

import com.gs.fw.common.mithra.cache.offheap.MithraOffHeapDataObject;

import java.io.*;




public interface MithraDataObject
{

    public boolean changed(MithraDataObject newData);
    public void copyNonPkAttributes(MithraDataObject newData);
    public MithraDataObject copy();
    public MithraDataObject copy(boolean copyRelationships);

    public void zSetDataVersion(byte version);
    public byte zGetDataVersion();
    public void zIncrementDataVersion();

    public void zSerializeFullData(ObjectOutput out) throws IOException;
    public void zDeserializeFullData(ObjectInput in) throws IOException, ClassNotFoundException;

    public void zWriteDataClassName(ObjectOutput out) throws IOException;
    public String zReadDataClassName(ObjectInput in) throws IOException, ClassNotFoundException;

    public boolean zAsOfAttributesFromEquals(MithraDataObject other);

    public boolean zAsOfAttributesChanged(MithraDataObject other);
    public boolean zNonPrimaryKeyAttributesChanged(MithraDataObject newData);
    public boolean zNonPrimaryKeyAttributesChanged(MithraDataObject newData, double tolerance);
    public void zSerializePrimaryKey(ObjectOutput out) throws IOException;

    public void zDeserializePrimaryKey(ObjectInput in) throws IOException, ClassNotFoundException;

    public MithraObjectPortal zGetMithraObjectPortal();

    public MithraObjectPortal zGetMithraObjectPortal(int hierarchyDepth);

    public String zGetPrintablePrimaryKey();

    public boolean zHasIdentity();
    public Number zGetIdentityValue();
    public void zSetIdentity(Number identityValue);

    public boolean hasSamePrimaryKeyIgnoringAsOfAttributes(MithraDataObject mithraDataObject);

    public void clearRelationships();

    public boolean zHasSameNullPrimaryKeyAttributes(MithraDataObject mithraDataObject);

    public void clearAllDirectRefs();

    public void zSerializeRelationships(ObjectOutputStream out) throws IOException;

    public void zDeserializeRelationships(ObjectInputStream in) throws IOException, ClassNotFoundException;

    public MithraOffHeapDataObject zCopyOffHeap();

    public String zGetSerializationClassName();
}
