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

package com.gs.fw.common.mithra.test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;



public class ExceptionCatchingCountDownLatch
{
    private final CountDownLatch latch;

    public ExceptionCatchingCountDownLatch(int count)
    {
        this.latch = new CountDownLatch(count);
    }

    public void countDown()
    {
        this.latch.countDown();
    }

    public void await()
    {
        try
        {
            this.latch.await();
        }
        catch (InterruptedException e)
        {
            throw new RuntimeException(e);
        }
    }

    public void await(long timeOut, TimeUnit unit)
    {
        try
        {
            this.latch.await(timeOut, unit);
        }
        catch (InterruptedException e)
        {
            throw new RuntimeException(e);
        }
    }
}
