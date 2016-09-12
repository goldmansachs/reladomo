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

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectStreamException;


public class SingleColumnTupleShortAttribute<Owner> extends SingleColumnShortAttribute<Owner>
{

    private int pos;
    private boolean isNullable; // isNullable is transient in Attribute, so we have to carry it here for serialization
    private TupleTempContext tupleTempContext;

    public SingleColumnTupleShortAttribute(int pos, boolean isNullable, TupleTempContext tupleTempContext)
    {
        super("c"+pos, "", "c"+pos, "", "", isNullable, false, tupleTempContext.getRelatedFinder(), null, true, false);
        this.tupleTempContext = tupleTempContext;
        this.pos = pos;
        this.isNullable = isNullable;
    }

    @Override
    public void setValue(Owner o, Short newValue)
    {
        ((ArrayTuple)o).setAttribute(this.pos, newValue);
    }

    public boolean isAttributeNull(Owner o)
    {
        return ((Tuple)o).isAttributeNull(this.pos);
    }

    public void setValueNull(Owner o)
    {
        throw new RuntimeException("not implemented");
    }

    public short shortValueOf(Owner o)
    {
        return ((Tuple)o).getAttributeAsShort(this.pos);
    }

    public void setShortValue(Owner o, short newValue)
    {
        throw new RuntimeException("not implemented");
    }

    protected Object writeReplace() throws ObjectStreamException
    {
        return this;
    }

    private synchronized void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException
    {
        in.defaultReadObject();
        this.setAll("c"+pos, "", "", this.isNullable, this.tupleTempContext.getRelatedFinder(), null, true);
        this.setColumnName("c"+pos);
    }
}
