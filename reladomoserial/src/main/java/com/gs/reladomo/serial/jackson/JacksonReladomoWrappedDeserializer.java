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

package com.gs.reladomo.serial.jackson;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.*;
import com.fasterxml.jackson.databind.deser.ContextualDeserializer;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;
import com.gs.fw.common.mithra.MithraDatedTransactionalObject;
import com.gs.fw.common.mithra.MithraObject;
import com.gs.fw.common.mithra.MithraTransactionalObject;
import com.gs.fw.common.mithra.util.serializer.ReladomoDeserializer;
import com.gs.fw.common.mithra.util.serializer.Serialized;
import com.gs.reladomo.serial.json.IntDateParser;
import com.gs.reladomo.serial.json.JsonDeserializerState;

import java.io.IOException;
import java.util.Date;

public class JacksonReladomoWrappedDeserializer extends StdDeserializer<Serialized<?>> implements ContextualDeserializer
{
    private JavaType valueType;

    public JacksonReladomoWrappedDeserializer()
    {
        super(Serialized.class);
    }

    @Override
    public JsonDeserializer<?> createContextual(DeserializationContext ctxt, BeanProperty property) throws JsonMappingException
    {
        JavaType wrapperType = null;
        if (property != null)
        {
            wrapperType = property.getType();
        }
        else
        {
            wrapperType = ctxt.getContextualType().getContentType();
        }
        if (wrapperType == null)
        {
            wrapperType = ctxt.getContextualType();
        }
        if (wrapperType == null)
        {
            return this;
        }
        JavaType valueType = wrapperType.containedType(0);
        JacksonReladomoWrappedDeserializer deserializer = new JacksonReladomoWrappedDeserializer();
        if (valueType == null)
        {
            valueType = wrapperType;
        }
        deserializer.valueType = valueType;
        return deserializer;
    }

    @Override
    public Serialized<?> deserialize(JsonParser parser, DeserializationContext ctxt) throws IOException
    {
        ReladomoDeserializer deserializer;
        if (this.valueType == null)
        {
            deserializer = new ReladomoDeserializer();
        }
        else
        {
            Class<?> rawClass = this.valueType.getRawClass();
            if (MithraObject.class == rawClass || MithraTransactionalObject.class == rawClass ||
                    MithraDatedTransactionalObject.class == rawClass)
            {
                deserializer = new ReladomoDeserializer();
            }
            else
            {
                deserializer = new ReladomoDeserializer(rawClass);
            }
        }

        deserializer.setIgnoreUnknown();

        DateParser dateParser = new DateParser(ctxt);

        JsonDeserializerState state = JsonDeserializerState.NormalParserState.INSTANCE;
        do
        {
            JsonToken jsonToken = parser.getCurrentToken();

            if (JsonToken.START_OBJECT.equals(jsonToken))
            {
                state = state.startObject(deserializer);
            }
            else if (JsonToken.END_OBJECT.equals(jsonToken))
            {
                state = state.endObject(deserializer);
            }
            else if (JsonToken.START_ARRAY.equals(jsonToken))
            {
                state = state.startArray(deserializer);
            }
            else if (JsonToken.END_ARRAY.equals(jsonToken))
            {
                state = state.endArray(deserializer);
            }
            else if (JsonToken.FIELD_NAME.equals(jsonToken))
            {
                state = state.fieldName(parser.getCurrentName(), deserializer);
            }
            else if (JsonToken.VALUE_EMBEDDED_OBJECT.equals(jsonToken))
            {
                state = state.valueEmbeddedObject(deserializer);
            }
            else if (JsonToken.VALUE_FALSE.equals(jsonToken))
            {
                state = state.valueFalse(deserializer);
            }
            else if (JsonToken.VALUE_TRUE.equals(jsonToken))
            {
                state = state.valueTrue(deserializer);
            }
            else if (JsonToken.VALUE_NULL.equals(jsonToken))
            {
                state = state.valueNull(deserializer);
            }
            else if (JsonToken.VALUE_STRING.equals(jsonToken))
            {
                state = state.valueString(parser.getValueAsString(), deserializer);
            }
            else if (JsonToken.VALUE_NUMBER_INT.equals(jsonToken))
            {
                state = state.valueNumberInt(parser.getValueAsString(), deserializer, dateParser
                );
            }
            else if (JsonToken.VALUE_NUMBER_FLOAT.equals(jsonToken))
            {
                state = state.valueNumberFloat(parser.getValueAsString(), deserializer);
            }
            parser.nextToken();
        } while (!parser.isClosed());
        return deserializer.getDeserializedResult();
    }

    private class DateParser implements IntDateParser
    {
        private DeserializationContext ctxt;

        public DateParser(DeserializationContext ctxt)
        {
            this.ctxt = ctxt;
        }

        @Override
        public Date parseIntAsDate(String value) throws IOException
        {
            return _parseDate(value, ctxt);
        }
    }
}
