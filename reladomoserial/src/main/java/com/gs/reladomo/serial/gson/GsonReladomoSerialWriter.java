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

package com.gs.reladomo.serial.gson;

import com.google.gson.JsonNull;
import com.gs.fw.common.mithra.MithraList;
import com.gs.fw.common.mithra.MithraObject;
import com.gs.fw.common.mithra.attribute.Attribute;
import com.gs.fw.common.mithra.finder.AbstractRelatedFinder;
import com.gs.fw.common.mithra.util.Time;
import com.gs.fw.common.mithra.util.serializer.SerialWriter;

import java.math.BigDecimal;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.TimeZone;

public class GsonReladomoSerialWriter implements SerialWriter<GsonRelodomoSerialContext>
{
    private static final SimpleDateFormat TIMESTAMP_FORMAT = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");
    private static final SimpleDateFormat DATE_FORMAT = new SimpleDateFormat("yyyy-MM-dd");

    static
    {
        TIMESTAMP_FORMAT.setTimeZone(TimeZone.getTimeZone("UTC"));
    }

    private static String timestampToJson(Timestamp timestamp)
    {
        synchronized (TIMESTAMP_FORMAT)
        {
            return TIMESTAMP_FORMAT.format(timestamp);
        }
    }

    private static String dateToJson(Date timestamp)
    {
        synchronized (DATE_FORMAT)
        {
            return DATE_FORMAT.format(timestamp);
        }
    }


    @Override
    public void writeBoolean(GsonRelodomoSerialContext context, String attributeName, boolean value)
    {
        context.getCurrentResultAsObject().addProperty(attributeName, value);
    }

    @Override
    public void writeByte(GsonRelodomoSerialContext context, String attributeName, byte value)
    {
        context.getCurrentResultAsObject().addProperty(attributeName, value);
    }

    @Override
    public void writeShort(GsonRelodomoSerialContext context, String attributeName, short value)
    {
        context.getCurrentResultAsObject().addProperty(attributeName, value);
    }

    @Override
    public void writeInt(GsonRelodomoSerialContext context, String attributeName, int value)
    {
        context.getCurrentResultAsObject().addProperty(attributeName, value);
    }

    @Override
    public void writeLong(GsonRelodomoSerialContext context, String attributeName, long value)
    {
        context.getCurrentResultAsObject().addProperty(attributeName, value);
    }

    @Override
    public void writeChar(GsonRelodomoSerialContext context, String attributeName, char value)
    {
        context.getCurrentResultAsObject().addProperty(attributeName, value);
    }

    @Override
    public void writeFloat(GsonRelodomoSerialContext context, String attributeName, float value)
    {
        context.getCurrentResultAsObject().addProperty(attributeName, value);
    }

    @Override
    public void writeDouble(GsonRelodomoSerialContext context, String attributeName, double value)
    {
        context.getCurrentResultAsObject().addProperty(attributeName, value);
    }

    @Override
    public void writeByteArray(GsonRelodomoSerialContext context, String attributeName, byte[] value)
    {
        context.getCurrentResultAsObject().add(attributeName, context.getGsonContext().serialize(value));
    }

    @Override
    public void writeBigDecimal(GsonRelodomoSerialContext context, String attributeName, BigDecimal value)
    {
        context.getCurrentResultAsObject().addProperty(attributeName, value);
    }

    @Override
    public void writeTimestamp(GsonRelodomoSerialContext context, String attributeName, Timestamp value)
    {
        context.getCurrentResultAsObject().addProperty(attributeName, timestampToJson(value));
    }

    @Override
    public void writeDate(GsonRelodomoSerialContext context, String attributeName, Date value)
    {
        context.getCurrentResultAsObject().addProperty(attributeName, dateToJson(value));
    }

    @Override
    public void writeString(GsonRelodomoSerialContext context, String attributeName, String value)
    {
        context.getCurrentResultAsObject().addProperty(attributeName, value);
    }

    @Override
    public void writeTime(GsonRelodomoSerialContext context, String attributeName, Time value)
    {
        context.getCurrentResultAsObject().addProperty(attributeName, value.toString());
    }

    @Override
    public void writeObject(GsonRelodomoSerialContext context, String attributeName, Object value)
    {
        context.getCurrentResultAsObject().add(attributeName, context.getGsonContext().serialize(value));
    }

    @Override
    public void writeLink(GsonRelodomoSerialContext context, String linkName, Attribute[] dependentAttributes)
    {
        //todo implement link
    }

    @Override
    public void writeNull(GsonRelodomoSerialContext context, String attributeName, Class type)
    {
        context.getCurrentResultAsObject().add(attributeName, JsonNull.INSTANCE);
    }

    @Override
    public void startReladomoObject(MithraObject reladomoObject, GsonRelodomoSerialContext context)
    {
        context.pushNewObject(null);
    }

    @Override
    public void endReladomoObject(MithraObject reladomoObject, GsonRelodomoSerialContext context)
    {
        context.pop();
    }

    @Override
    public void startRelatedObject(GsonRelodomoSerialContext context, String attributeName, AbstractRelatedFinder finder, MithraObject value)
    {
        context.pushNewObject(attributeName);
    }

    @Override
    public void endRelatedObject(GsonRelodomoSerialContext context, String attributeName, AbstractRelatedFinder finder, MithraObject value)
    {
    }

    @Override
    public void startRelatedReladomoList(GsonRelodomoSerialContext context, String attributeName, AbstractRelatedFinder finder, MithraList valueList)
    {
        context.pushNewObject(attributeName);
    }

    @Override
    public void endRelatedReladomoList(GsonRelodomoSerialContext context, String attributeName, AbstractRelatedFinder finder, MithraList valueList)
    {
        context.pop();
    }

    @Override
    public void startMetadata(MithraObject reladomoObject, GsonRelodomoSerialContext context)
    {
//        context.pushNewObject("_rdoMetaData");
    }

    @Override
    public void writeMetadataEnd(MithraObject reladomoObject, GsonRelodomoSerialContext context)
    {
//        context.pop();
    }

    @Override
    public void startAttributes(GsonRelodomoSerialContext context, int size)
    {

    }

    @Override
    public void endAttributes(GsonRelodomoSerialContext context)
    {

    }

    @Override
    public void startRelationships(GsonRelodomoSerialContext context, int size)
    {

    }

    @Override
    public void endRelationships(GsonRelodomoSerialContext context)
    {

    }

    @Override
    public void startLinks(GsonRelodomoSerialContext gsonRelodomoSerialContext, int size)
    {

    }

    @Override
    public void endLinks(GsonRelodomoSerialContext gsonRelodomoSerialContext)
    {

    }

    @Override
    public void startAnnotatedMethod(MithraObject reladomoObject, GsonRelodomoSerialContext context, int size)
    {

    }

    @Override
    public void endAnnotatedMethod(MithraObject reladomoObject, GsonRelodomoSerialContext context)
    {

    }

    @Override
    public void startReladomoList(MithraList reladomoList, GsonRelodomoSerialContext context)
    {
        context.pushNewObject(null);
    }

    @Override
    public void endReladomoList(MithraList reladomoList, GsonRelodomoSerialContext context)
    {
        context.pop();
    }

    @Override
    public void startReladomoListMetatdata(MithraList reladomoList, GsonRelodomoSerialContext context)
    {
//        context.pushNewObject("_rdoMetaData");
    }

    @Override
    public void endReladomoListMedatadata(MithraList reladomoList, GsonRelodomoSerialContext context)
    {
//        context.pop();
    }

    @Override
    public void startReladomoListItem(MithraList reladomoList, GsonRelodomoSerialContext context, int index, MithraObject reladomoObject)
    {

    }

    @Override
    public void endReladomoListItem(MithraList reladomoList, GsonRelodomoSerialContext gsonRelodomoSerialContext, int index, MithraObject reladomoObject)
    {

    }

    @Override
    public void startReladomoListElements(MithraList reladomoList, GsonRelodomoSerialContext context)
    {
        context.pushNewArray("elements");
    }

    @Override
    public void endReladomoListElements(MithraList reladomoList, GsonRelodomoSerialContext context)
    {
        context.pop();
    }

    @Override
    public void startListAnnotatedMethods(MithraList reladomoList, GsonRelodomoSerialContext context, int numberOfAnnotatedMethods)
    {

    }

    @Override
    public void endListAnnotatedMethods(MithraList reladomoList, GsonRelodomoSerialContext context)
    {

    }
}
