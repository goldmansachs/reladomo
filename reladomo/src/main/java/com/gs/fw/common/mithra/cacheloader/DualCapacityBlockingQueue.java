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

package com.gs.fw.common.mithra.cacheloader;


import java.util.ArrayDeque;

public class DualCapacityBlockingQueue<E>
{
    private final ArrayDeque<E> queue = new ArrayDeque<E>();

    private final int capacity;
    private boolean stopped = false;

    public DualCapacityBlockingQueue(int capacity)
    {
        this.capacity = capacity;
    }

    public synchronized void addUnlimited(E e)
    {
        if (stopped) return;
        queue.add(e);
        this.notifyAll();
    }

    public synchronized void put(E e) throws InterruptedException
    {
        while (this.queue.size() >= capacity && !stopped)
        {
            this.wait();
        }
        addUnlimited(e);
    }

    public synchronized E take() throws InterruptedException
    {
        while (this.queue.isEmpty() && !stopped)
        {
            this.wait();
        }
        if (stopped) return null;

        E x = this.queue.removeFirst();

        if (queue.size() < capacity)
        {
            this.notifyAll();
        }
        return x;
    }

    public synchronized void shutdown()
    {
        this.stopped = true;
        this.queue.clear();

        this.notifyAll();
    }

    public long size()
    {
        return this.queue.size();
    }
}
