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

package com.gs.fw.common.mithra.tempobject;

import com.gs.collections.api.block.function.Function;
import com.gs.fw.common.mithra.MithraTransactionalObject;
import com.gs.fw.common.mithra.extractor.*;
import com.gs.fw.common.mithra.transaction.UpdateOperation;
import com.gs.fw.common.mithra.util.Time;

import java.io.IOException;
import java.io.ObjectOutput;
import java.io.ObjectStreamException;
import java.math.BigDecimal;
import java.sql.Timestamp;
import java.util.Date;
import java.util.List;


public class UpdateOperationTupleAdaptor extends TupleImpl
{
    private transient Object keyHolder; // The keyHolder is used for the data before update. If THRU_Z is a part of the key - the keyHolder need to hold to THRU_Z before the update.
    private transient Object updateHolder;
    private transient Extractor[] delegateAttributes;
    private int keyAttributeCount;

    public static Function<UpdateOperation, TupleImpl> createFactory (final Extractor[] delegateAttributes)
    {
        return new Function<UpdateOperation, TupleImpl>()
        {
            @Override
            public TupleImpl valueOf(UpdateOperation o)
            {
                return new UpdateOperationTupleAdaptor(o, delegateAttributes);
            }
        };
    }

    public UpdateOperationTupleAdaptor(UpdateOperation updateOperation, Extractor[] delegateAttributes)
    {
        this.updateHolder = updateOperation.getMithraObject().zGetTxDataForRead();
        this.keyHolder = updateOperation.getMithraObject().zGetNonTxData();
        if (this.keyHolder == null)
        {
            this.keyHolder = this.updateHolder;
        }

        this.delegateAttributes = delegateAttributes;
        this.keyAttributeCount = delegateAttributes.length - updateOperation.getUpdates().size();
    }

    public Object getAttribute(int index)
    {
        return delegateAttributes[index].valueOf(index < keyAttributeCount ? keyHolder : updateHolder);
    }

    public boolean isAttributeNull(int index)
    {
        return delegateAttributes[index].isAttributeNull(index < keyAttributeCount ? keyHolder : updateHolder);
    }

    public boolean getAttributeAsBoolean(int index)
    {
        return ((BooleanExtractor)delegateAttributes[index]).booleanValueOf(index < keyAttributeCount ? keyHolder : updateHolder);
    }

    public byte getAttributeAsByte(int index)
    {
        return ((ByteExtractor)delegateAttributes[index]).byteValueOf(index < keyAttributeCount ? keyHolder : updateHolder);
    }

    public short getAttributeAsShort(int index)
    {
        return ((ShortExtractor)delegateAttributes[index]).shortValueOf(index < keyAttributeCount ? keyHolder : updateHolder);
    }

    public char getAttributeAsChar(int index)
    {
        return ((CharExtractor)delegateAttributes[index]).charValueOf(index < keyAttributeCount ? keyHolder : updateHolder);
    }

    public int getAttributeAsInt(int index)
    {
        return ((IntExtractor)delegateAttributes[index]).intValueOf(index < keyAttributeCount ? keyHolder : updateHolder);
    }

    public long getAttributeAsLong(int index)
    {
        return ((LongExtractor)delegateAttributes[index]).longValueOf(index < keyAttributeCount ? keyHolder : updateHolder);
    }

    public float getAttributeAsFloat(int index)
    {
        return ((FloatExtractor)delegateAttributes[index]).floatValueOf(index < keyAttributeCount ? keyHolder : updateHolder);
    }

    public double getAttributeAsDouble(int index)
    {
        return ((DoubleExtractor)delegateAttributes[index]).doubleValueOf(index < keyAttributeCount ? keyHolder : updateHolder);
    }

    public String getAttributeAsString(int index)
    {
        return ((StringExtractor)delegateAttributes[index]).stringValueOf(index < keyAttributeCount ? keyHolder : updateHolder);
    }

    public Date getAttributeAsDate(int index)
    {
        return ((DateExtractor)delegateAttributes[index]).dateValueOf(index < keyAttributeCount ? keyHolder : updateHolder);
    }

    public Time getAttributeAsTime(int index)
    {
        return ((TimeExtractor)delegateAttributes[index]).timeValueOf(index < keyAttributeCount ? keyHolder : updateHolder);
    }

    public Timestamp getAttributeAsTimestamp(int index)
    {
        return ((TimestampExtractor)delegateAttributes[index]).timestampValueOf(index < keyAttributeCount ? keyHolder : updateHolder);
    }

    public byte[] getAttributeAsByteArray(int index)
    {
        return ((ByteArrayExtractor)delegateAttributes[index]).byteArrayValueOf(index < keyAttributeCount ? keyHolder : updateHolder);
    }

    public BigDecimal getAttributeAsBigDecimal(int index)
    {
        return ((BigDecimalExtractor)delegateAttributes[index]).bigDecimalValueOf(index < keyAttributeCount ? keyHolder : updateHolder);
    }

    public void writeToStream(ObjectOutput os) throws IOException
    {
        for(int i=0;i<this.delegateAttributes.length;i++)
        {
            os.writeObject(this.delegateAttributes[i].valueOf(i < keyAttributeCount ? keyHolder : updateHolder));
        }
    }

    protected Object writeReplace() throws ObjectStreamException
    {
        Object[] values = new Object[this.delegateAttributes.length];
        for(int i=0;i<this.delegateAttributes.length;i++)
        {
            values[i] = this.delegateAttributes[i].valueOf(i < keyAttributeCount ? keyHolder : updateHolder);
        }
        return new ArrayTuple(values);
    }

    public String zGetPrintablePrimaryKey()
    {
        StringBuffer buf = new StringBuffer(this.delegateAttributes.length * 8);
        for(int i=0;i<this.delegateAttributes.length;i++)
        {
            if (i > 0) buf.append(',');
            buf.append(delegateAttributes[i].valueOf(i < keyAttributeCount ? keyHolder : updateHolder));
        }
        return buf.toString();
    }
}
