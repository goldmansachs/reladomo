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

import java.io.ByteArrayOutputStream;
import java.io.OutputStream;
import java.io.IOException;
import java.io.ObjectOutput;

public class WrittableByteArrayOutputStream extends ByteArrayOutputStream
{
    public WrittableByteArrayOutputStream(int size)
    {
        super(size);
    }

    public void writeToStream(OutputStream out) throws IOException
    {
        out.write(this.buf, 0, this.count);
    }

    public void writeToStream(ObjectOutput out) throws IOException
    {
        out.write(this.buf, 0, this.count);
    }
}
