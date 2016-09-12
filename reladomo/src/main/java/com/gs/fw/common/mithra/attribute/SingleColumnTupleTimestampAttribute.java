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

package com.gs.fw.common.mithra.attribute;

import com.gs.fw.common.mithra.tempobject.ArrayTuple;
import com.gs.fw.common.mithra.tempobject.Tuple;
import com.gs.fw.common.mithra.tempobject.TupleTempContext;
import com.gs.fw.common.mithra.MithraDataObject;

import java.io.ObjectStreamException;
import java.io.ObjectInputStream;
import java.io.IOException;
import java.sql.Timestamp;


public class SingleColumnTupleTimestampAttribute<Owner> extends SingleColumnTimestampAttribute<Owner>
{

    private int pos;
    private byte conversionType = CONVERT_NONE;
    private byte precision;
    private boolean setAsString = false;
    private Timestamp infinity;
    private TupleTempContext tupleTempContext;

    public SingleColumnTupleTimestampAttribute(int pos, TupleTempContext tupleTempContext, SingleColumnTimestampAttribute prototype)
    {
        super("c"+pos, "", "c"+pos, "", "", false, false, tupleTempContext.getRelatedFinder(), null,
                true, false, prototype.getConversionType(), prototype.isSetAsString(), prototype.isAsOfAttributeTo(),
                prototype.getAsOfAttributeInfinity(), prototype.getPrecision());
        this.tupleTempContext = tupleTempContext;
        this.pos = pos;
    }

    @Override
    public void setValue(Owner o, Timestamp newValue)
    {
        ((ArrayTuple)o).setAttribute(this.pos, newValue);
    }

    public boolean hasSameVersion(MithraDataObject first, MithraDataObject second)
    {
        return true;
    }

    public void setTimestampValue(Object o, Timestamp newValue)
    {
        throw new RuntimeException("not implemented");
    }

    public Timestamp timestampValueOf(Object o)
    {
        return ((Tuple)o).getAttributeAsTimestamp(this.pos);
    }

    protected Object writeReplace() throws ObjectStreamException
    {
        return this;
    }

    private synchronized void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException
    {
        in.defaultReadObject();
        this.setAll("c"+pos, "", "", false, this.tupleTempContext.getRelatedFinder(), null, true);
        this.setColumnName("c"+pos);
        this.setTimestampProperties(this.conversionType, this.setAsString, false, this.infinity, precision);
    }
}
