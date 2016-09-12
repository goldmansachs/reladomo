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


import sun.misc.Unsafe;

import java.util.ArrayDeque;

public class PeekableQueue
{
    private volatile Object head;
    private ArrayDeque deque = new ArrayDeque(100);

    private static final Unsafe UNSAFE = MithraUnsafe.getUnsafe();
    private static long HEAD_OFFSET;

    static
    {
        try
        {
            HEAD_OFFSET = UNSAFE.objectFieldOffset(PeekableQueue.class.getDeclaredField("head"));
        }
        catch (NoSuchFieldException e)
        {
            throw new RuntimeException("could not get head offset", e);
        }
    }

    private boolean casHead(Object expected, Object newValue)
    {
        return UNSAFE.compareAndSwapObject(this, HEAD_OFFSET, expected, newValue);
    }

    public void add(Object o)
    {
        while(head == null)
        {
            if (casHead(null, o))
            {
                synchronized (this)
                {
                    notifyAll();
                }
                return;
            }
        }
        synchronized (deque)
        {
            deque.addLast(o);
        }
        moveDequeToHead(null);
    }

    private void moveDequeToHead(Object oldHead)
    {
        boolean notify = false;
        while (head == oldHead)
        {
            synchronized (deque)
            {
                if (!deque.isEmpty())
                {
                    Object first = deque.getFirst();
                    if (casHead(oldHead, first))
                    {
                        deque.removeFirst();
                        notify = true;
                        break;
                    }
                }
                else
                {
                    if (casHead(oldHead, null))
                    {
                        break;
                    }
                }
            }
        }
        if (notify)
        {
            synchronized (this)
            {
                notifyAll();
            }
        }
    }

    public Object peekBlockIfEmpty()
    {
        while(true)
        {
            Object h = head;
            if (h != null)
            {
                return h;
            }
            synchronized (this)
            {
                while (head == null)
                {
                    try
                    {
                        this.wait();
                    }
                    catch (InterruptedException e)
                    {
                        //ignore
                    }
                }
            }
        }
    }

    public void removeIfSame(Object toRemove)
    {
        moveDequeToHead(toRemove);
    }
}
