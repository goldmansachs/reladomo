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

package com.gs.fw.common.mithra.generator.util;

import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.io.IOException;
import java.io.ByteArrayInputStream;
import java.util.zip.CRC32;


public class FullFileBuffer
{

    private byte[] buf = new byte[8096];
    private int size;

    public void bufferFile(InputStream fileInputStream, int size) throws IOException
    {
        this.size = size;
        if (size > buf.length)
        {
            buf = new byte[size];
        }
        int bytesRead = 0;
        while(bytesRead < size)
        {
            int current = fileInputStream.read(buf, bytesRead, size - bytesRead);
            if (current < 0)
            {
                throw new RuntimeException("expecting to read "+size+" bytes, but could only read "+bytesRead);
            }
            bytesRead += current;
        }
    }

    public InputStream getBufferedInputStream()
    {
        return new ByteArrayInputStream(buf, 0, size);
    }
    
    public void updateCrc(CRC32 crc32)
    {
        synchronized (crc32)
        {
            crc32.update(buf, 0, size);
        }
    }
}
