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

package com.gs.reladomo.serial.json;

import com.gs.fw.common.mithra.attribute.AsOfAttribute;
import com.gs.fw.common.mithra.attribute.DateAttribute;
import com.gs.fw.common.mithra.attribute.TimestampAttribute;
import com.gs.fw.common.mithra.util.serializer.ReladomoDeserializer;
import com.gs.fw.common.mithra.util.serializer.ReladomoSerializationContext;

import java.io.IOException;
import java.sql.Timestamp;
import java.util.Date;

public abstract class JsonDeserializerState
{
    public JsonDeserializerState startObject(ReladomoDeserializer deserializer) throws IOException
    {
        throw new RuntimeException("Shouldn't call startObject in "+this.getClass().getSimpleName());
    }
    public JsonDeserializerState endObject(ReladomoDeserializer deserializer) throws IOException
    {
        throw new RuntimeException("Shouldn't call endObject in "+this.getClass().getSimpleName());
    }
    public JsonDeserializerState startArray(ReladomoDeserializer deserializer) throws IOException
    {
        throw new RuntimeException("Shouldn't call startArray in "+this.getClass().getSimpleName());
    }
    public JsonDeserializerState endArray(ReladomoDeserializer deserializer) throws IOException
    {
        throw new RuntimeException("Shouldn't call endArray in "+this.getClass().getSimpleName());
    }
    public JsonDeserializerState fieldName(String fieldName, ReladomoDeserializer deserializer) throws IOException
    {
        throw new RuntimeException("Shouldn't call fieldName in "+this.getClass().getSimpleName());
    }
    public JsonDeserializerState valueEmbeddedObject(ReladomoDeserializer deserializer) throws IOException
    {
        throw new RuntimeException("Shouldn't call valueEmbeddedObject in "+this.getClass().getSimpleName());
    }
    public JsonDeserializerState valueTrue(ReladomoDeserializer deserializer) throws IOException
    {
        throw new RuntimeException("Shouldn't call valueTrue in "+this.getClass().getSimpleName());
    }
    public JsonDeserializerState valueFalse(ReladomoDeserializer deserializer) throws IOException
    {
        throw new RuntimeException("Shouldn't call valueFalse in "+this.getClass().getSimpleName());
    }
    public JsonDeserializerState valueNull(ReladomoDeserializer deserializer) throws IOException
    {
        throw new RuntimeException("Shouldn't call valueNull in "+this.getClass().getSimpleName());
    }
    public JsonDeserializerState valueString(String value, ReladomoDeserializer deserializer) throws IOException
    {
        throw new RuntimeException("Shouldn't call valueString in "+this.getClass().getSimpleName());
    }

    public JsonDeserializerState valueTimestamp(Timestamp value, ReladomoDeserializer deserializer) throws IOException
    {
        throw new RuntimeException("Shouldn't call valueString in "+this.getClass().getSimpleName());
    }

    public JsonDeserializerState valueNumberInt(String value, ReladomoDeserializer deserializer, IntDateParser intDateParser) throws IOException
    {
        throw new RuntimeException("Shouldn't call valueNumberInt in "+this.getClass().getSimpleName());
    }
    public JsonDeserializerState valueNumberFloat(String value, ReladomoDeserializer deserializer) throws IOException
    {
        throw new RuntimeException("Shouldn't call valueNumberFloat in "+this.getClass().getSimpleName());
    }

    public static class NormalParserState extends JsonDeserializerState
    {
        public static NormalParserState INSTANCE = new NormalParserState();
        @Override
        public JsonDeserializerState startObject(ReladomoDeserializer deserializer) throws IOException
        {
            deserializer.startObject();
            return this;
        }

        @Override
        public JsonDeserializerState endObject(ReladomoDeserializer deserializer) throws IOException
        {
            deserializer.endObjectOrList();
            return this;
        }

        @Override
        public JsonDeserializerState fieldName(String fieldName, ReladomoDeserializer deserializer) throws IOException
        {
            if (fieldName.equals(ReladomoSerializationContext.RELADOMO_CLASS_NAME))
            {
                return ClassNameState.INSTANCE;
            }
            else if (fieldName.equals(ReladomoSerializationContext.RELADOMO_STATE))
            {
                return ObjectStateState.INSTANCE;
            }
            else
            {
                ReladomoDeserializer.FieldOrRelation fieldOrRelation = deserializer.startFieldOrRelationship(fieldName);
                if (ReladomoDeserializer.FieldOrRelation.Unknown.equals(fieldOrRelation))
                {
                    return this;
                }
                else if (ReladomoDeserializer.FieldOrRelation.ToManyRelationship.equals(fieldOrRelation))
                {
                    return new ToManyState(this);
                }
            }
            
            return this;
        }

        @Override
        public JsonDeserializerState endArray(ReladomoDeserializer deserializer) throws IOException
        {
            deserializer.endListElements();
            return this;
        }

        @Override
        public JsonDeserializerState valueTrue(ReladomoDeserializer deserializer) throws IOException
        {
            deserializer.setBooleanField(true);
            return this;
        }

        @Override
        public JsonDeserializerState valueFalse(ReladomoDeserializer deserializer) throws IOException
        {
            deserializer.setBooleanField(false);
            return this;
        }

        @Override
        public JsonDeserializerState valueNull(ReladomoDeserializer deserializer) throws IOException
        {
            deserializer.setFieldOrRelationshipNull();
            return this;
        }

        @Override
        public JsonDeserializerState valueString(String value, ReladomoDeserializer deserializer) throws IOException
        {
            deserializer.parseFieldFromString(value);
            return this;
        }

        @Override
        public JsonDeserializerState valueTimestamp(Timestamp value, ReladomoDeserializer deserializer) throws IOException
        {
            deserializer.setTimestampField(value);
            return this;
        }

        @Override
        public JsonDeserializerState valueNumberInt(String value, ReladomoDeserializer deserializer, IntDateParser intDateParser) throws IOException
        {
            if (deserializer.getCurrentAttribute() instanceof TimestampAttribute ||
                    deserializer.getCurrentAttribute() instanceof AsOfAttribute)
            {
                Date date = intDateParser.parseIntAsDate(value);
                deserializer.setTimestampField(new Timestamp(date.getTime()));
            }
            else if (deserializer.getCurrentAttribute() instanceof DateAttribute)
            {
                Date date = intDateParser.parseIntAsDate(value);
                deserializer.setDateField(new java.sql.Date(date.getTime()));
            }
            else
            {
                deserializer.parseFieldFromString(value);
            }
            return this;
        }

        @Override
        public JsonDeserializerState valueNumberFloat(String value, ReladomoDeserializer deserializer) throws IOException
        {
            deserializer.parseFieldFromString(value);
            return this;
        }
    }
    
    public static class ListStartState extends IgnoreState
    {
        public static ListStartState INSTANCE = new ListStartState();

        public ListStartState()
        {
            super(NormalParserState.INSTANCE);
        }

        @Override
        public JsonDeserializerState startObject(ReladomoDeserializer deserializer) throws IOException
        {
            deserializer.startList();
            return this;
        }

        @Override
        public JsonDeserializerState fieldName(String fieldName, ReladomoDeserializer deserializer) throws IOException
        {
            if (fieldName.equals(ReladomoSerializationContext.RELADOMO_CLASS_NAME))
            {
                return new ClassNameStateWithPrevious(this);
            }
            else if (fieldName.equals("elements"))
            {
                return new InListState(this);
            }
            return this;
        }
    }

    public static class ClassNameStateWithPrevious extends JsonDeserializerState
    {
        private JsonDeserializerState previous;

        public ClassNameStateWithPrevious (JsonDeserializerState previous)
        {
            this.previous = previous;
        }

        @Override
        public JsonDeserializerState valueString(String value, ReladomoDeserializer deserializer) throws IOException
        {
            deserializer.storeReladomoClassName(value);
            return previous;
        }
    }
    
    public static class ClassNameState extends JsonDeserializerState
    {
        public static ClassNameState INSTANCE = new ClassNameState();
        @Override
        public JsonDeserializerState valueString(String value, ReladomoDeserializer deserializer) throws IOException
        {
            deserializer.storeReladomoClassName(value);
            return NormalParserState.INSTANCE;
        }
    }
    
    public static class ObjectStateState extends JsonDeserializerState
    {
        public static ObjectStateState INSTANCE = new ObjectStateState();

        @Override
        public JsonDeserializerState valueString(String value, ReladomoDeserializer deserializer) throws IOException
        {
            return valueNumberInt(value, deserializer, null);
        }

        @Override
        public JsonDeserializerState valueNumberInt(String value, ReladomoDeserializer deserializer, IntDateParser intDateParser) throws IOException
        {
            deserializer.setReladomoObjectState(Integer.parseInt(value));
            return NormalParserState.INSTANCE;
        }
    }


    public static class IgnoreState extends JsonDeserializerState
    {
        protected JsonDeserializerState previous;

        public IgnoreState (JsonDeserializerState previous)
        {
            this.previous = previous;
        }

        @Override
        public JsonDeserializerState startObject(ReladomoDeserializer deserializer) throws IOException
        {
            return new IgnoreState(this);
        }

        @Override
        public JsonDeserializerState endObject(ReladomoDeserializer deserializer) throws IOException
        {
            return previous;
        }

        @Override
        public JsonDeserializerState startArray(ReladomoDeserializer deserializer) throws IOException
        {
            return this;
        }

        @Override
        public JsonDeserializerState endArray(ReladomoDeserializer deserializer) throws IOException
        {
            return this;
        }

        @Override
        public JsonDeserializerState fieldName(String fieldName, ReladomoDeserializer deserializer) throws IOException
        {
            return this;
        }

        @Override
        public JsonDeserializerState valueEmbeddedObject(ReladomoDeserializer deserializer) throws IOException
        {
            return this;
        }

        @Override
        public JsonDeserializerState valueTrue(ReladomoDeserializer deserializer) throws IOException
        {
            return this;
        }

        @Override
        public JsonDeserializerState valueFalse(ReladomoDeserializer deserializer) throws IOException
        {
            return this;
        }

        @Override
        public JsonDeserializerState valueNull(ReladomoDeserializer deserializer) throws IOException
        {
            return this;
        }

        @Override
        public JsonDeserializerState valueTimestamp(Timestamp value, ReladomoDeserializer deserializer) throws IOException
        {
            return this;
        }

        @Override
        public JsonDeserializerState valueString(String value, ReladomoDeserializer deserializer) throws IOException
        {
            return this;
        }

        @Override
        public JsonDeserializerState valueNumberInt(String value, ReladomoDeserializer deserializer, IntDateParser intDateParser) throws IOException
        {
            return this;
        }

        @Override
        public JsonDeserializerState valueNumberFloat(String value, ReladomoDeserializer deserializer) throws IOException
        {
            return this;
        }
    }

    public static class ToManyState extends IgnoreState
    {

        public ToManyState (JsonDeserializerState previous)
        {
            super(previous);
        }

        @Override
        public JsonDeserializerState startObject(ReladomoDeserializer deserializer) throws IOException
        {
            deserializer.startList();
            return new WaitForElementsState(previous);
        }
    }

    public static class WaitForElementsState extends IgnoreState
    {
        public WaitForElementsState (JsonDeserializerState previous)
        {
            super(previous);
        }

        @Override
        public JsonDeserializerState fieldName(String fieldName, ReladomoDeserializer deserializer) throws IOException
        {
            if ("elements".equals(fieldName))
            {
                return new InListState(previous);
            }
            return this;
        }
    }

    public static class InListState extends JsonDeserializerState
    {
        public JsonDeserializerState previous;

        public InListState (JsonDeserializerState previous)
        {
            this.previous = previous;
        }

        @Override
        public JsonDeserializerState startArray(ReladomoDeserializer deserializer) throws IOException
        {
            deserializer.startListElements();
            return new InArrayState(previous);
        }
    }

    public static class InArrayState extends NormalParserState
    {
        public JsonDeserializerState previous;

        public InArrayState (JsonDeserializerState previous)
        {
            this.previous = previous;
        }

        @Override
        public JsonDeserializerState endArray(ReladomoDeserializer deserializer) throws IOException
        {
            deserializer.endListElements();
            return previous;
        }
    }
}
