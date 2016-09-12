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

package com.gs.fw.common.mithra.test.tinyproxy;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;

public class ObjectWithSerializationError implements Serializable
{
    private String contents;
    private boolean exceptionThrown;

    public ObjectWithSerializationError(String contents)
    {
        this.contents = contents;
    }

    public String getContents()
    {
        return this.contents;
    }

    private void writeObject(ObjectOutputStream out) throws IOException
    {
        byte[] bytes = this.contents.getBytes("ISO8859_1");
        out.writeInt(bytes.length);
        if (this.exceptionThrown)
        {
            out.write(bytes);
        }
        else
        {
            out.write(bytes, 0, bytes.length / 2);
            this.exceptionThrown = true;
            throw new IOException();
        }
    }

    private void readObject(ObjectInputStream in) throws IOException
    {
        int len = in.readInt();
        byte[] buffer = new byte[len];
        int readSoFar = 0;
        while (readSoFar < len)
        {
            readSoFar += in.read(buffer, readSoFar, len - readSoFar);
        }
        this.contents = new String(buffer, "ISO8859_1");
    }
}
