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

package com.gs.fw.common.mithra.util;

import com.gs.fw.common.mithra.MithraDataObject;
import com.gs.fw.common.mithra.MithraObject;
import com.gs.fw.common.mithra.MithraObjectPortal;
import com.gs.fw.common.mithra.attribute.*;
import com.gs.fw.common.mithra.finder.RelatedFinder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;


public class VerboseSerializer
{
    private static final Logger logger = LoggerFactory.getLogger(VerboseSerializer.class);

    private RelatedFinder finder;
    private Class dataClass;

    private static final Map<Class, Byte> CLASS_TO_TYPE_MAP = new HashMap<Class, Byte>();
    private static final Attribute[] SKIPPER_ATTRIBUTES = new Attribute[13];

    private static final byte BOOLEAN_TYPE = 1;
    private static final byte BYTE_TYPE = 2;
    private static final byte CHAR_TYPE = 3;
    private static final byte SHORT_TYPE = 4;
    private static final byte INTEGER_TYPE = 5;
    private static final byte LONG_TYPE = 6;
    private static final byte FLOAT_TYPE = 7;
    private static final byte DOUBLE_TYPE = 8;
    private static final byte STRING_TYPE = 9;
    private static final byte DATE_TYPE = 10;
    private static final byte TIMESTAMP_TYPE = 11;
    private static final byte TIME_TYPE = 12;

    static
    {
        CLASS_TO_TYPE_MAP.put(Boolean.class, Byte.valueOf(BOOLEAN_TYPE));
        CLASS_TO_TYPE_MAP.put(Byte.class, Byte.valueOf(BYTE_TYPE));
        CLASS_TO_TYPE_MAP.put(Character.class, Byte.valueOf(CHAR_TYPE));
        CLASS_TO_TYPE_MAP.put(Short.class, Byte.valueOf(SHORT_TYPE));
        CLASS_TO_TYPE_MAP.put(Integer.class, Byte.valueOf(INTEGER_TYPE));
        CLASS_TO_TYPE_MAP.put(Long.class, Byte.valueOf(LONG_TYPE));
        CLASS_TO_TYPE_MAP.put(Float.class, Byte.valueOf(FLOAT_TYPE));
        CLASS_TO_TYPE_MAP.put(Double.class, Byte.valueOf(DOUBLE_TYPE));
        CLASS_TO_TYPE_MAP.put(String.class, Byte.valueOf(STRING_TYPE));
        CLASS_TO_TYPE_MAP.put(java.util.Date.class, Byte.valueOf(DATE_TYPE));
        CLASS_TO_TYPE_MAP.put(Timestamp.class, Byte.valueOf(TIMESTAMP_TYPE));
        CLASS_TO_TYPE_MAP.put(Time.class, Byte.valueOf(TIME_TYPE));

        SKIPPER_ATTRIBUTES[BOOLEAN_TYPE] = new SkippingBooleanAttribute();
        SKIPPER_ATTRIBUTES[BYTE_TYPE] = new SkippingByteAttribute();
        SKIPPER_ATTRIBUTES[SHORT_TYPE] = new SkippingShortAttribute();
        SKIPPER_ATTRIBUTES[CHAR_TYPE] = new SkippingCharAttribute();
        SKIPPER_ATTRIBUTES[INTEGER_TYPE] = new SkippingIntegerAttribute();
        SKIPPER_ATTRIBUTES[LONG_TYPE] = new SkippingLongAttribute();
        SKIPPER_ATTRIBUTES[DOUBLE_TYPE] = new SkippingDoubleAttribute();
        SKIPPER_ATTRIBUTES[FLOAT_TYPE] = new SkippingFloatAttribute();
        SKIPPER_ATTRIBUTES[STRING_TYPE] = new SkippingStringAttribute();
        SKIPPER_ATTRIBUTES[TIMESTAMP_TYPE] = new SkippingTimestampAttribute();
        SKIPPER_ATTRIBUTES[DATE_TYPE] = new SkippingDateAttribute();
        SKIPPER_ATTRIBUTES[TIME_TYPE] = new SkippingTimeAttribute();

    }

    public VerboseSerializer(RelatedFinder finder)
    {
        this.finder = finder;
    }

    public VerboseSerializer(RelatedFinder finder, Class dataClass)
    {
        this.finder = finder;
        this.dataClass = dataClass;
        if (dataClass.isInterface())
        {
            try
            {
                this.dataClass = Class.forName(dataClass.getName()+"$"+dataClass.getSimpleName()+"OnHeap");
            }
            catch (ClassNotFoundException e)
            {
                throw new RuntimeException("Could not find on heap version of "+dataClass.getClass().getName(), e);
            }
        }
    }

    protected void writeHeader(ObjectOutput out) throws IOException
    {
        Attribute[] persistentAttributes = this.finder.getPersistentAttributes();
        Attribute sourceAttribute = this.finder.getSourceAttribute();
        int total = persistentAttributes.length;
        if (sourceAttribute != null)
        {
            total++;
        }
        out.writeInt(total);
        if (sourceAttribute != null)
        {
            writeAttributeHeader(out, sourceAttribute);
        }
        for (Attribute persistentAttribute : persistentAttributes)
        {
            writeAttributeHeader(out, persistentAttribute);
        }
    }

    private void writeAttributeHeader(ObjectOutput out, Attribute attribute)
            throws IOException
    {
        out.writeUTF(attribute.getAttributeName());
        out.writeByte(CLASS_TO_TYPE_MAP.get(attribute.valueType()));
    }


    private String getClassName()
    {
        return this.finder.getClass().getName();
    }

    public void writeObjects(List objects, ObjectOutput out) throws IOException
    {
        this.writeHeader(out);
        Attribute[] persistentAttributes = this.finder.getPersistentAttributes();
        Attribute sourceAttribute = this.finder.getSourceAttribute();
        out.writeInt(objects.size());
        for (Object o : objects)
        {
            if (o instanceof MithraObject)
            {
                o = ((MithraObject) o).zGetCurrentData();
            }
            if (sourceAttribute != null)
            {
                sourceAttribute.serializeValue(o, out);
            }
            for (Attribute persistentAttribute : persistentAttributes)
            {
                persistentAttribute.serializeValue(o, out);
            }
        }
    }

    public List readObjectsAsDataObjects(ObjectInput in) throws IOException
    {
        DataObjectsIterator it = new DataObjectsIterator(in, true);
        ArrayList result = new ArrayList(it.getSize());
        while (it.hasNext())
        {
            result.add(it.next());
        }
        return result;
    }

    public Iterator getDataObjectsIterator(ObjectInput in, boolean validateLocalAttributes) throws IOException
    {
        return new DataObjectsIterator(in, validateLocalAttributes);
    }

    @SuppressWarnings("unchecked")
    private final class DataObjectsIterator implements Iterator
    {
        private final int size;
        private final ObjectInput in;
        private final boolean validateLocalAttributes;
        private Attribute[] incomingAttributes;
        private int counter;

        private DataObjectsIterator(ObjectInput in, boolean validateLocalAttributes) throws IOException
        {
            this.in = in;
            this.validateLocalAttributes = validateLocalAttributes;
            readHeader(in);
            this.size = in.readInt();
        }

        public int getSize()
        {
            return this.size;
        }

        private void readHeader(ObjectInput in) throws IOException
        {
            int attributes = in.readInt();
            this.incomingAttributes = new Attribute[attributes];
            int found = 0;
            for (int i = 0; i < attributes; i++)
            {
                String name = in.readUTF();
                byte type = in.readByte();
                Attribute attribute = VerboseSerializer.this.finder.getAttributeByName(name);
                if (attribute == null)
                {
                    attribute = SKIPPER_ATTRIBUTES[type];
                    logger.warn(getClassName() + " could not find attribute " + name + " of type " + attribute.valueType().getName() + ". Skipping it!");
                }
                else
                {
                    if (CLASS_TO_TYPE_MAP.get(attribute.valueType()) != type)
                    {
                        String msg = VerboseSerializer.this.finder.getClass().getName() + " found mismatched types for attribute " + name + " incoming type " + SKIPPER_ATTRIBUTES[type].valueType().getName() +
                                " local type " + attribute.valueType().getName();
                        logger.error(msg);
                        throw new IOException(msg);
                    }
                    found++;
                }
                this.incomingAttributes[i] = attribute;
            }
            checkLocalAttributes(found);
        }

        private void checkLocalAttributes(int found) throws IOException
        {
            Attribute[] persistentAttributes = VerboseSerializer.this.finder.getPersistentAttributes();
            int localCount = persistentAttributes.length;
            Attribute sourceAttribute = VerboseSerializer.this.finder.getSourceAttribute();
            if (sourceAttribute != null)
            {
                localCount++;
            }
            if (found < localCount)
            {
                Set<String> localAttributes = new HashSet();
                for (Attribute persistentAttribute : persistentAttributes)
                {
                    localAttributes.add(persistentAttribute.getAttributeName());
                }
                if (sourceAttribute != null)
                {
                    localAttributes.add(sourceAttribute.getAttributeName());
                }
                for (Attribute incomingAttribute : this.incomingAttributes)
                {
                    localAttributes.remove(incomingAttribute.getAttributeName());
                }
                String msg = getClassName() + " could not find the following local attributes in the incoming object:";
                for (String localAttribute : localAttributes)
                {
                    msg += " " + localAttribute;
                }
                if (this.validateLocalAttributes)
                {
                    logger.error(msg);
                    throw new IOException(msg);
                }
                else
                {
                    logger.warn(msg);
                }
            }
        }

        public boolean hasNext()
        {
            return this.counter < this.size;
        }

        public Object next()
        {
            this.counter++;
            Object data = null;
            try
            {
                data = VerboseSerializer.this.dataClass.newInstance();
            }
            catch (Exception e)
            {
                logger.error("could not instantiate data", e);
                IOException ioException = new IOException("could not instantiate data");
                ioException.initCause(e);
                throw new RuntimeException(ioException);
            }

            try
            {
                for (Attribute incomingAttribute : this.incomingAttributes)
                {

                    incomingAttribute.deserializeValue(data, this.in);
                }
            }
            catch (Exception e)
            {
                logger.error("Error inflating data object ", e);
                throw new RuntimeException(e);
            }

            return data;
        }

        public void remove()
        {
            throw new UnsupportedOperationException("Modifications not allowed");
        }
    }

    private static class SkippingBooleanAttribute extends SingleColumnBooleanAttribute
    {
        @Override
        public MithraObjectPortal getOwnerPortal() { return null; }
        public Object readResolve() { return null; }
        public void setValueNull(Object o) {}
        public boolean isAttributeNull(Object o) { return false; }
        public boolean booleanValueOf(Object o) { return false; }
        public void setBooleanValue(Object o, boolean newValue) {}
    }

    private static class SkippingByteAttribute extends SingleColumnByteAttribute
    {
        @Override
        public MithraObjectPortal getOwnerPortal() { return null; }
        public Object readResolve() { return null; }
        public void setValueNull(Object o) {}
        public boolean isAttributeNull(Object o) { return false; }
        public byte byteValueOf(Object o) { return 0; }
        public void setByteValue(Object o, byte newValue) {}
    }

    private static class SkippingCharAttribute extends SingleColumnCharAttribute
    {
        @Override
        public MithraObjectPortal getOwnerPortal() { return null; }
        public Object readResolve() { return null; }
        public void setValueNull(Object o) {}
        public boolean isAttributeNull(Object o) { return false; }
        public char charValueOf(Object o) { return 0; }
        public void setCharValue(Object o, char newValue) {}
    }

    private static class SkippingShortAttribute extends SingleColumnShortAttribute
    {
        @Override
        public MithraObjectPortal getOwnerPortal() { return null; }
        public Object readResolve() { return null; }
        public void setValueNull(Object o) {}
        public boolean isAttributeNull(Object o) { return false; }
        public short shortValueOf(Object o) { return 0; }
        public void setShortValue(Object o, short newValue) {}
    }

    private static class SkippingIntegerAttribute extends SingleColumnIntegerAttribute
    {
        @Override
        public MithraObjectPortal getOwnerPortal() { return null; }
        public Object readResolve() { return null; }
        public void setValueNull(Object o) {}
        public boolean isAttributeNull(Object o) { return false; }
        public int intValueOf(Object o) { return 0; }
        public void setIntValue(Object o, int newValue) {}
        public boolean hasSameVersion(MithraDataObject first, MithraDataObject second) { return false; }
        public boolean isSequenceSet(Object o) { return false; }
    }

    private static class SkippingLongAttribute extends SingleColumnLongAttribute

    {
        @Override
        public MithraObjectPortal getOwnerPortal() { return null; }
        public Object readResolve() { return null; }
        public void setValueNull(Object o) {}
        public boolean isAttributeNull(Object o) { return false; }
        public long longValueOf(Object o) { return 0; }
        public void setLongValue(Object o, long newValue) {}
        public boolean isSequenceSet(Object o) { return false; }
    }

    private static class SkippingFloatAttribute extends SingleColumnFloatAttribute
    {
        @Override
        public MithraObjectPortal getOwnerPortal() { return null; }
        public Object readResolve() { return null; }
        public void setValueNull(Object o) {}
        public boolean isAttributeNull(Object o) { return false; }
        public float floatValueOf(Object o) { return 0; }
        public void setFloatValue(Object o, float newValue) {}
    }

    private static class SkippingDoubleAttribute extends SingleColumnDoubleAttribute
    {
        @Override
        public MithraObjectPortal getOwnerPortal() { return null; }
        public Object readResolve() { return null; }
        public void setValueNull(Object o) {}
        public boolean isAttributeNull(Object o) { return false; }
        public double doubleValueOf(Object o) { return 0; }
        public void setDoubleValue(Object o, double newValue) {}
    }

    private static class SkippingStringAttribute extends SingleColumnStringAttribute
    {
        @Override
        public MithraObjectPortal getOwnerPortal() { return null; }
        public Object readResolve() { return null; }
        @Override
        public void setValueNull(Object o) {}
        @Override
        public boolean isAttributeNull(Object o) { return false; }
        public String stringValueOf(Object o) { return null; }
        public void setStringValue(Object o, String newValue) {}
    }

    private static class SkippingTimestampAttribute extends SingleColumnTimestampAttribute
    {
        @Override
        public MithraObjectPortal getOwnerPortal() { return null; }
        public Object readResolve() { return null; }
        @Override
        public void setValueNull(Object o) {}
        @Override
        public boolean isAttributeNull(Object o) { return false; }
        public Timestamp timestampValueOf(Object o) { return null; }
        public void setTimestampValue(Object o, Timestamp newValue) {}
        public boolean hasSameVersion(MithraDataObject first, MithraDataObject second) { return false; }
    }

    private static class SkippingDateAttribute extends SingleColumnDateAttribute
    {
        @Override
        public MithraObjectPortal getOwnerPortal() { return null; }
        public Object readResolve() { return null; }
        @Override
        public void setValueNull(Object o) {}
        @Override
        public boolean isAttributeNull(Object o) { return false; }
        public java.util.Date dateValueOf(Object o) { return null; }
        public void setDateValue(Object o, java.util.Date newValue) {}
    }

    private static class SkippingTimeAttribute extends SingleColumnTimeAttribute
    {
        @Override
        public MithraObjectPortal getOwnerPortal() { return null; }
        public Object readResolve() { return null; }
        @Override
        public void setValueNull(Object o) {}
        @Override
        public boolean isAttributeNull(Object o) { return false; }
        public Time timeValueOf(Object o) { return null; }
        public void setTimeValue(Object o, Time newValue) {}
    }
}
