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

package com.gs.fw.common.mithra.notification;

import java.io.ByteArrayInputStream;
import java.io.ObjectInput;
import java.io.IOException;


public class ReadableByteArrayInputStream extends ByteArrayInputStream
{

    public ReadableByteArrayInputStream(int size)
    {
        super(new byte[size]);
        this.count = 0;
    }

    public void readFromStream(ObjectInput in, int size) throws IOException
    {
        if (size > buf.length)
        {
            buf = new byte[size];
        }
        int read = 0;
        while (read < size)
        {
            int moreRead = in.read(buf, read, size - read);
            if (moreRead < 0) throw new IOException("Could not read "+size+" bytes from stream");
            read += moreRead;
        }
        this.count = size;
        this.pos = 0;
    }
}
