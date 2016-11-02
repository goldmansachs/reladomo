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

package com.gs.fw.common.mithra.util.serializer;

import com.gs.fw.common.mithra.MithraList;
import com.gs.fw.common.mithra.MithraObject;
import com.gs.fw.common.mithra.attribute.Attribute;
import com.gs.fw.common.mithra.finder.AbstractRelatedFinder;
import com.gs.fw.common.mithra.util.Time;

import java.io.IOException;
import java.math.BigDecimal;
import java.sql.Timestamp;
import java.util.Date;

public interface SerialWriter<C extends ReladomoSerializationContext>
{
    public void writeBoolean(C context, String attributeName, boolean value) throws IOException;
    public void writeByte(C context, String attributeName, byte value) throws IOException;
    public void writeShort(C context, String attributeName, short value) throws IOException;
    public void writeInt(C context, String attributeName, int value) throws IOException;
    public void writeLong(C context, String attributeName, long value) throws IOException;
    public void writeChar(C context, String attributeName, char value) throws IOException;
    public void writeFloat(C context, String attributeName, float value) throws IOException;
    public void writeDouble(C context, String attributeName, double value) throws IOException;
    public void writeByteArray(C context, String attributeName, byte[] value) throws IOException;
    public void writeBigDecimal(C context, String attributeName, BigDecimal value) throws IOException;
    public void writeTimestamp(C context, String attributeName, Timestamp value) throws IOException;
    public void writeDate(C context, String attributeName, Date value) throws IOException;
    public void writeString(C context, String attributeName, String value) throws IOException;
    public void writeTime(C context, String attributeName, Time value) throws IOException;

    public void writeObject(C context, String attributeName, Object value) throws IOException;

    public void writeLink(C context, String linkName, Attribute[] dependentAttributes) throws IOException;

    public void writeNull(C context, String attributeName, Class type) throws IOException;

    public void startReladomoObject(MithraObject reladomoObject, C context) throws IOException;
    public void endReladomoObject(MithraObject reladomoObject, C context) throws IOException;

    public void startRelatedObject(C context, String attributeName, AbstractRelatedFinder finder, MithraObject value) throws IOException;
    public void endRelatedObject(C context, String attributeName, AbstractRelatedFinder finder, MithraObject value) throws IOException;

    public void startRelatedReladomoList(C context, String attributeName, AbstractRelatedFinder finder, MithraList valueList) throws IOException;
    public void endRelatedReladomoList(C context, String attributeName, AbstractRelatedFinder finder, MithraList valueList) throws IOException;

    public void startMetadata(MithraObject reladomoObject, C context) throws IOException;
    public void writeMetadataEnd(MithraObject reladomoObject, C context) throws IOException;

    public void startAttributes(C context, int size) throws IOException;
    public void endAttributes(C context) throws IOException;

    public void startRelationships(C context, int size) throws IOException;
    public void endRelationships(C context) throws IOException;

    public void startLinks(C c, int size) throws IOException;
    public void endLinks(C c) throws IOException;

    public void startAnnotatedMethod(MithraObject reladomoObject, C context, int size) throws IOException;
    public void endAnnotatedMethod(MithraObject reladomoObject, C context) throws IOException;

    public void startReladomoList(MithraList reladomoList, C context) throws IOException;

    public void endReladomoList(MithraList reladomoList, C context) throws IOException;

    public void startReladomoListMetatdata(MithraList reladomoList, C context) throws IOException;

    public void endReladomoListMedatadata(MithraList reladomoList, C context) throws IOException;

    public void startReladomoListItem(MithraList reladomoList, C context, int index, MithraObject reladomoObject) throws IOException;

    public void endReladomoListItem(MithraList reladomoList, C c, int index, MithraObject reladomoObject) throws IOException;

    public void startReladomoListElements(MithraList reladomoList, C context) throws IOException;
    public void endReladomoListElements(MithraList reladomoList, C context) throws IOException;

    public void startListAnnotatedMethods(MithraList reladomoList, C context, int numberOfAnnotatedMethods);

    public void endListAnnotatedMethods(MithraList reladomoList, C context);
}
