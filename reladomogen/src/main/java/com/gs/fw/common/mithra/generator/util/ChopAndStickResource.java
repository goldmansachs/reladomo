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

import java.util.concurrent.Semaphore;


public class ChopAndStickResource
{

    private Semaphore cpuSemaphore;
    private Semaphore ioSemaphore;
    private SerialResource serialResource;

    public ChopAndStickResource(Semaphore cpuSemaphore, Semaphore ioSemaphore, SerialResource serialResource)
    {
        this.cpuSemaphore = cpuSemaphore;
        this.ioSemaphore = ioSemaphore;
        this.serialResource = serialResource;
    }

    public void acquireCpuResource()
    {
        acquireSemaphore(cpuSemaphore);
    }

    public void releaseCpuResource()
    {
        cpuSemaphore.release();
    }

    public void acquireIoResource()
    {
        acquireSemaphore(ioSemaphore);
    }

    public void releaseIoResource()
    {
        ioSemaphore.release();
    }

    public void acquireSerialResource(int resourceNumber)
    {
        serialResource.acquire(resourceNumber);
    }

    public void releaseSerialResource()
    {
        serialResource.release();
    }

    public void resetSerialResource()
    {
        serialResource.reset();
    }

    private void acquireSemaphore(Semaphore semaphore)
    {
        while(true)
        {
            try
            {
                semaphore.acquire();
                break;
            }
            catch (InterruptedException e)
            {
                //ignore
            }
        }
    }
}
