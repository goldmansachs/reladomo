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

package com.gs.fw.common.mithra.remote;

import com.gs.fw.common.mithra.tempobject.Tuple;
import com.gs.fw.common.mithra.tempobject.ArrayTuple;

import java.io.Externalizable;
import java.io.ObjectInput;
import java.io.IOException;
import java.io.ObjectOutput;
import java.util.List;
import java.util.ArrayList;


public class ExternalizableTupleList implements Externalizable
{

    private List tupleList;
    private int tupleSize;

    public ExternalizableTupleList(List tupleList, int tupleSize)
    {
        this.tupleList = tupleList;
        this.tupleSize = tupleSize;
    }

    public ExternalizableTupleList()
    {
        // for externalizable
    }

    public List getTupleList()
    {
        return tupleList;
    }

    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException
    {
        int len = in.readInt();
        this.tupleList = new ArrayList(len);
        this.tupleSize = in.readInt();
        for(int i=0;i<len;i++)
        {
            tupleList.add(new ArrayTuple(this.tupleSize, in));
        }
    }

    public void writeExternal(ObjectOutput out) throws IOException
    {
        int len = tupleList.size();
        out.writeInt(len);
        out.writeInt(tupleSize);
        for(int i=0;i<len;i++)
        {
            Tuple t = (Tuple) tupleList.get(i);
            t.writeToStream(out);
        }
    }
}
