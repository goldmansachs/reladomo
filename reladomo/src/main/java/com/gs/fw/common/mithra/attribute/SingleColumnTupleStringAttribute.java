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

import java.io.ObjectStreamException;
import java.io.ObjectInputStream;
import java.io.IOException;


public class SingleColumnTupleStringAttribute<Owner> extends SingleColumnStringAttribute<Owner>
{

    private int pos;
    private TupleTempContext tupleTempContext;

    public SingleColumnTupleStringAttribute(int pos, int maxLength, TupleTempContext tupleTempContext, boolean stringTrim)
    {
        super("c"+pos, "", "c"+pos, "", "", false, false, tupleTempContext.getRelatedFinder(), null, true, false, maxLength, stringTrim);
        this.tupleTempContext = tupleTempContext;
        this.pos = pos;
    }

    @Override
    public void setValue(Owner o, String newValue)
    {
        ((ArrayTuple)o).setAttribute(this.pos, newValue);
    }

    public void setStringValue(Object o, String newValue)
    {
        throw new RuntimeException("not implemented");
    }

    public String stringValueOf(Object o)
    {
        return ((Tuple)o).getAttributeAsString(this.pos);
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
    }
}
