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

package com.gs.fw.common.mithra.cache.offheap;


import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

public class MasterRetrieveStringResult implements Externalizable
{
    private int[] masterRefs;
    private String[] masterStrings;

    public MasterRetrieveStringResult()
    {
        // for externalizable
    }

    public MasterRetrieveStringResult(int size)
    {
        this.masterRefs = new int[size];
        this.masterStrings = new String[size];
    }

    public int[] getMasterRefs()
    {
        return masterRefs;
    }

    public String[] getMasterStrings()
    {
        return masterStrings;
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException
    {
        int size = in.readInt();
        this.masterRefs = new int[size];
        this.masterStrings = new String[size];
        for(int i=0;i<size;i++)
        {
            this.masterRefs[i] = in.readInt();
        }
        for(int i=0;i<size;i++)
        {
            this.masterStrings[i] = readString(in);
        }
    }

    private String readString(ObjectInput in) throws IOException
    {
        int length = in.readInt();
        if (length == -1) return null;
        char[] toRead = new char[length];
        for(int i=0;i<length;i++)
        {
            toRead[i] = in.readChar();
        }
        return new String(toRead);
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException
    {
        out.writeInt(this.masterRefs.length);
        for(int i=0;i<masterRefs.length;i++)
        {
            out.writeInt(this.masterRefs[i]);
        }
        for(int i=0;i<masterStrings.length;i++)
        {
            String str = this.masterStrings[i];
            if (str == null)
            {
                out.writeInt(-1);
                continue;
            }
            out.writeInt(str.length());
            for(int j=0;j<str.length();j++)
            {
                out.writeChar(str.charAt(j));
            }
        }

    }

    public void addString(int index, int masterRef, String string)
    {
        this.masterRefs[index] = masterRef;
        this.masterStrings[index] = string;
    }
}
