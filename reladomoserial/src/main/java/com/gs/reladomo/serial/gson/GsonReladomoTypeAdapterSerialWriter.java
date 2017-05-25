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

import com.google.gson.Gson;
import com.gs.fw.common.mithra.MithraList;
import com.gs.fw.common.mithra.MithraObject;
import com.gs.fw.common.mithra.attribute.Attribute;
import com.gs.fw.common.mithra.finder.AbstractRelatedFinder;
import com.gs.fw.common.mithra.util.Time;
import com.gs.fw.common.mithra.util.serializer.SerialWriter;

import java.io.IOException;
import java.math.BigDecimal;
import java.sql.Timestamp;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.TimeZone;

public class GsonReladomoTypeAdapterSerialWriter implements SerialWriter<GsonReladomoTypeAdapterContext>
{
    private static final SimpleDateFormat TIMESTAMP_FORMAT = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");
    private static final SimpleDateFormat DATE_FORMAT = new SimpleDateFormat("yyyy-MM-dd");

    static
    {
        TIMESTAMP_FORMAT.setTimeZone(TimeZone.getTimeZone("UTC"));
    }

    public static String timestampToJson(Timestamp timestamp)
    {
        synchronized (TIMESTAMP_FORMAT)
        {
            return TIMESTAMP_FORMAT.format(timestamp);
        }
    }

    public static String dateToJson(Date timestamp)
    {
        synchronized (DATE_FORMAT)
        {
            return DATE_FORMAT.format(timestamp);
        }
    }

    public static Timestamp jsonToTimestamp(String json) throws IOException
    {
        synchronized (TIMESTAMP_FORMAT)
        {
            try
            {
                return new Timestamp(TIMESTAMP_FORMAT.parse(json).getTime());
            }
            catch (ParseException e)
            {
                throw new IOException("Could not parse '"+json+"' for format "+TIMESTAMP_FORMAT.toString(), e);
            }
        }
    }

    @Override
    public void writeBoolean(GsonReladomoTypeAdapterContext context, String attributeName, boolean value) throws IOException
    {
        context.getJsonWriter().name(attributeName).value(value);
    }

    @Override
    public void writeByte(GsonReladomoTypeAdapterContext context, String attributeName, byte value) throws IOException
    {
        context.getJsonWriter().name(attributeName).value(value);
    }

    @Override
    public void writeShort(GsonReladomoTypeAdapterContext context, String attributeName, short value) throws IOException
    {
        context.getJsonWriter().name(attributeName).value(value);
    }

    @Override
    public void writeInt(GsonReladomoTypeAdapterContext context, String attributeName, int value) throws IOException
    {
        context.getJsonWriter().name(attributeName).value(value);
    }

    @Override
    public void writeLong(GsonReladomoTypeAdapterContext context, String attributeName, long value) throws IOException
    {
        context.getJsonWriter().name(attributeName).value(value);
    }

    @Override
    public void writeChar(GsonReladomoTypeAdapterContext context, String attributeName, char value) throws IOException
    {
        context.getJsonWriter().name(attributeName).value(String.valueOf(value));
    }

    @Override
    public void writeFloat(GsonReladomoTypeAdapterContext context, String attributeName, float value) throws IOException
    {
        context.getJsonWriter().name(attributeName).value(value);
    }

    @Override
    public void writeDouble(GsonReladomoTypeAdapterContext context, String attributeName, double value) throws IOException
    {
        context.getJsonWriter().name(attributeName).value(value);
    }

    @Override
    public void writeByteArray(GsonReladomoTypeAdapterContext context, String attributeName, byte[] value) throws IOException
    {
        //todo base64 encode
    }

    @Override
    public void writeBigDecimal(GsonReladomoTypeAdapterContext context, String attributeName, BigDecimal value) throws IOException
    {
        context.getJsonWriter().name(attributeName).value(value);
    }

    @Override
    public void writeTimestamp(GsonReladomoTypeAdapterContext context, String attributeName, Timestamp value) throws IOException
    {
        context.getJsonWriter().name(attributeName).value(timestampToJson(value));
    }

    @Override
    public void writeDate(GsonReladomoTypeAdapterContext context, String attributeName, Date value) throws IOException
    {
        context.getJsonWriter().name(attributeName).value(dateToJson(value));
    }

    @Override
    public void writeString(GsonReladomoTypeAdapterContext context, String attributeName, String value) throws IOException
    {
        context.getJsonWriter().name(attributeName).value(value);
    }

    @Override
    public void writeTime(GsonReladomoTypeAdapterContext context, String attributeName, Time value) throws IOException
    {
        context.getJsonWriter().name(attributeName).value(value.toString());
    }

    @Override
    public void writeObject(GsonReladomoTypeAdapterContext context, String attributeName, Object value) throws IOException
    {
        context.getJsonWriter().name(attributeName).jsonValue(new Gson().toJson(value));
    }

    @Override
    public void writeLink(GsonReladomoTypeAdapterContext context, String linkName, Attribute[] dependentAttributes) throws IOException
    {
        //todo implement link
    }

    @Override
    public void writeNull(GsonReladomoTypeAdapterContext context, String attributeName, Class type) throws IOException
    {
        context.getJsonWriter().name(attributeName).nullValue();
    }

    @Override
    public void startReladomoObject(MithraObject reladomoObject, GsonReladomoTypeAdapterContext context) throws IOException
    {
        context.getJsonWriter().beginObject();
    }

    @Override
    public void endReladomoObject(MithraObject reladomoObject, GsonReladomoTypeAdapterContext context) throws IOException
    {
        context.getJsonWriter().endObject();
    }

    @Override
    public void startRelatedObject(GsonReladomoTypeAdapterContext context, String attributeName, AbstractRelatedFinder finder, MithraObject value) throws IOException
    {
        context.getJsonWriter().name(attributeName);
    }

    @Override
    public void endRelatedObject(GsonReladomoTypeAdapterContext context, String attributeName, AbstractRelatedFinder finder, MithraObject value) throws IOException
    {
    }

    @Override
    public void startRelatedReladomoList(GsonReladomoTypeAdapterContext context, String attributeName, AbstractRelatedFinder finder, MithraList valueList) throws IOException
    {
        context.getJsonWriter().name(attributeName).beginObject();
    }

    @Override
    public void endRelatedReladomoList(GsonReladomoTypeAdapterContext context, String attributeName, AbstractRelatedFinder finder, MithraList valueList) throws IOException
    {
        context.getJsonWriter().endObject();
    }

    @Override
    public void startMetadata(MithraObject reladomoObject, GsonReladomoTypeAdapterContext context)
    {
//        context.pushNewObject("_rdoMetaData");
    }

    @Override
    public void writeMetadataEnd(MithraObject reladomoObject, GsonReladomoTypeAdapterContext context)
    {
//        context.pop();
    }

    @Override
    public void startAttributes(GsonReladomoTypeAdapterContext context, int size)
    {

    }

    @Override
    public void endAttributes(GsonReladomoTypeAdapterContext context)
    {

    }

    @Override
    public void startRelationships(GsonReladomoTypeAdapterContext context, int size)
    {

    }

    @Override
    public void endRelationships(GsonReladomoTypeAdapterContext context)
    {

    }

    @Override
    public void startLinks(GsonReladomoTypeAdapterContext GsonReladomoTypeAdapterContext, int size)
    {

    }

    @Override
    public void endLinks(GsonReladomoTypeAdapterContext GsonReladomoTypeAdapterContext)
    {

    }

    @Override
    public void startAnnotatedMethod(MithraObject reladomoObject, GsonReladomoTypeAdapterContext context, int size)
    {

    }

    @Override
    public void endAnnotatedMethod(MithraObject reladomoObject, GsonReladomoTypeAdapterContext context)
    {

    }

    @Override
    public void startReladomoList(MithraList reladomoList, GsonReladomoTypeAdapterContext context) throws IOException
    {
        context.getJsonWriter().beginObject();
    }

    @Override
    public void endReladomoList(MithraList reladomoList, GsonReladomoTypeAdapterContext context) throws IOException
    {
        context.getJsonWriter().endObject();
    }

    @Override
    public void startReladomoListMetatdata(MithraList reladomoList, GsonReladomoTypeAdapterContext context)
    {
//        context.pushNewObject("_rdoMetaData");
    }

    @Override
    public void endReladomoListMedatadata(MithraList reladomoList, GsonReladomoTypeAdapterContext context)
    {
//        context.pop();
    }

    @Override
    public void startReladomoListItem(MithraList reladomoList, GsonReladomoTypeAdapterContext context, int index, MithraObject reladomoObject)
    {

    }

    @Override
    public void endReladomoListItem(MithraList reladomoList, GsonReladomoTypeAdapterContext GsonReladomoTypeAdapterContext, int index, MithraObject reladomoObject)
    {

    }

    @Override
    public void startReladomoListElements(MithraList reladomoList, GsonReladomoTypeAdapterContext context) throws IOException
    {
        context.getJsonWriter().name("elements").beginArray();
    }

    @Override
    public void endReladomoListElements(MithraList reladomoList, GsonReladomoTypeAdapterContext context) throws IOException
    {
        context.getJsonWriter().endArray();
    }

    @Override
    public void startListAnnotatedMethods(MithraList reladomoList, GsonReladomoTypeAdapterContext context, int numberOfAnnotatedMethods)
    {

    }

    @Override
    public void endListAnnotatedMethods(MithraList reladomoList, GsonReladomoTypeAdapterContext context)
    {

    }
}
