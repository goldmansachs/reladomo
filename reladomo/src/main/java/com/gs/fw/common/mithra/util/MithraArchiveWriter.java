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

package com.gs.fw.common.mithra.util;

import com.gs.fw.common.mithra.MithraDatedObject;
import com.gs.fw.common.mithra.MithraObject;
import com.gs.fw.common.mithra.util.lz4.LZ4BlockOutputStream;

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.util.List;


public class MithraArchiveWriter
{

    private MithraRuntimeCacheController cacheController;
    private OutputStream out;
    private boolean started = false;
    private LZ4BlockOutputStream zip = null;
    private ObjectOutputStream oos;

    public MithraArchiveWriter(MithraRuntimeCacheController cacheController, OutputStream out)
    {
        this.cacheController = cacheController;
        this.out = new BufferedOutputStream(out, MithraRuntimeCacheController.POST_COMPRESS_BUFFER_SIZE);
    }

    private void startArchive() throws IOException
    {
        if (!started)
        {
            started = true;
            out.write(MithraRuntimeCacheController.CACHE_ARCHIVE_VERSION);
            out.write(MithraRuntimeCacheController.CACHE_COMPRESSION_VERSION);
            zip = new LZ4BlockOutputStream(out, false);
            oos = new ObjectOutputStream(new BufferedOutputStream(zip, MithraRuntimeCacheController.PRE_COMPRESS_BUFFER_SIZE));
            oos.writeObject(cacheController.getClassName());
            oos.writeInt(cacheController.getMithraObjectPortal().getFinder().getSerialVersionId());
        }
    }

    public void archiveObjectsIncrementally(List<? extends MithraObject> objects) throws IOException
    {
        startArchive();
        oos.writeInt(objects.size());
        for(int i=0;i<objects.size();i++)
        {
            MithraObject o = objects.get(i);
            if (o instanceof MithraDatedObject)
            {
                o.zGetCurrentData().zSerializeFullData(oos);
            }
            else
            {
                o.zSerializeFullData(oos);
            }
        }
    }

    public void finish() throws IOException
    {
        oos.writeInt(0);
        oos.flush();
        zip.finish();
        out.flush();
    }
}
