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

import com.google.gson.TypeAdapter;
import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonToken;
import com.google.gson.stream.JsonWriter;
import com.gs.fw.common.mithra.MithraList;
import com.gs.fw.common.mithra.MithraObject;
import com.gs.fw.common.mithra.attribute.Attribute;
import com.gs.fw.common.mithra.attribute.TimestampAttribute;
import com.gs.fw.common.mithra.util.serializer.ReladomoDeserializer;
import com.gs.fw.common.mithra.util.serializer.Serialized;
import com.gs.fw.common.mithra.util.serializer.SerializedList;
import com.gs.reladomo.serial.json.JsonDeserializerState;

import java.io.IOException;
import java.sql.Timestamp;

public class GsonWrappedListTypedAdapter<U extends MithraObject, T extends MithraList<U>> extends TypeAdapter<SerializedList<U, T>>
{
    private Class typeClass;

    public GsonWrappedListTypedAdapter(Class typeClass)
    {
        this.typeClass = typeClass;
    }

    @Override
    public void write(JsonWriter jsonWriter, SerializedList<U, T> serialized) throws IOException
    {
        GsonReladomoTypeAdapterSerialWriter writer = new GsonReladomoTypeAdapterSerialWriter();
        GsonReladomoTypeAdapterContext gsonRelodomoSerialContext = new GsonReladomoTypeAdapterContext(serialized.getConfig(), writer, jsonWriter);
        gsonRelodomoSerialContext.serializeReladomoList(serialized.getWrapped());
    }

    @Override
    public SerializedList<U, T> read(JsonReader jsonReader) throws IOException
    {
        ReladomoDeserializer deserializer;
        if (this.typeClass == null)
        {
            deserializer = new ReladomoDeserializer();
        }
        else
        {
            deserializer = new ReladomoDeserializer(typeClass);
        }
        deserializer.setIgnoreUnknown();
        JsonDeserializerState state = JsonDeserializerState.ListStartState.INSTANCE;
        while(true)
        {
            JsonToken nextToken = jsonReader.peek();
            //BEGIN_ARRAY, END_ARRAY, BEGIN_OBJECT, END_OBJECT, NAME, STRING, NUMBER, BOOLEAN, NULL, END_DOCUMENT;
            if (JsonToken.BEGIN_OBJECT == nextToken)
            {
                jsonReader.beginObject();
                state = state.startObject(deserializer);
            }
            else if (JsonToken.END_OBJECT == nextToken)
            {
                jsonReader.endObject();
                state = state.endObject(deserializer);
            }
            else if (JsonToken.BEGIN_ARRAY == nextToken)
            {
                jsonReader.beginArray();
                state = state.startArray(deserializer);
            }
            else if (JsonToken.END_ARRAY == nextToken)
            {
                jsonReader.endArray();
                state = state.endArray(deserializer);
            }
            else if (JsonToken.BOOLEAN == nextToken)
            {
                if (jsonReader.nextBoolean())
                {
                    state = state.valueTrue(deserializer);
                }
                else
                {
                    state = state.valueFalse(deserializer);
                }
            }
            else if (JsonToken.NAME == nextToken)
            {
                String name = jsonReader.nextName();
                state = state.fieldName(name, deserializer);
            }
            else if (JsonToken.NUMBER == nextToken)
            {
                String value = jsonReader.nextString();
                state = state.valueString(value, deserializer); // we do the parsing to avoid precision loss
            }
            else if (JsonToken.STRING == nextToken)
            {
                String value = jsonReader.nextString();
                Attribute attribute = deserializer.getCurrentAttribute();
                if (attribute instanceof TimestampAttribute)
                {
                    Timestamp timestamp = GsonReladomoTypeAdapterSerialWriter.jsonToTimestamp(value);
                    state = state.valueTimestamp(timestamp, deserializer);
                }
                else
                {
                    state = state.valueString(value, deserializer);
                }
            }
            else if (JsonToken.NULL == nextToken)
            {
                jsonReader.nextNull();
                state = state.valueNull(deserializer);
            }
            else if (JsonToken.END_DOCUMENT == nextToken)
            {
                break;
            }
        }
        return deserializer.getDeserializedResultAsList();
    }
}
