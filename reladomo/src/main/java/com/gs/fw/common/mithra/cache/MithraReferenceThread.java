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
// Portions copyright Hiroshi Ito. Licensed under Apache 2.0 license

package com.gs.fw.common.mithra.cache;

import com.gs.fw.common.mithra.extractor.IdentityExtractor;
import com.gs.fw.common.mithra.util.MithraFastList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.ref.WeakReference;
import java.util.concurrent.ConcurrentLinkedQueue;


public class MithraReferenceThread extends Thread
{
    private static final Logger logger = LoggerFactory.getLogger(MithraReferenceThread.class);

    private static final MithraReferenceThread instance = new MithraReferenceThread();

    private final MithraFastList<WeakReferenceListener> listeners = new MithraFastList<WeakReferenceListener>();
    private final ConcurrentLinkedQueue<WeakReferenceListener> queue = new ConcurrentLinkedQueue<WeakReferenceListener>();
    private final ConcurrentLinkedQueue<ReferenceListener> removeQueue = new ConcurrentLinkedQueue<ReferenceListener>();

    static
    {
        instance.start();
    }

    public MithraReferenceThread()
    {
        super("MithraReferenceThread");
        this.setDaemon(true);
    }

    public static MithraReferenceThread getInstance()
    {
        return instance;
    }

    public void addListener(ReferenceListener listener)
    {
        queue.add(new WeakReferenceListener(listener));
    }

    @Override
    public void run()
    {
        while(true)
        {
            if (this.listeners == null)
            {
                // only happens when badly written container code changes our private final variables
                logger.error("Detected misbehaving container and shutting down MithraReferenceThread");
                break;
            }
            try
            {
                WeakReferenceListener toAdd = queue.poll();
                while (toAdd != null)
                {
                    listeners.add(toAdd);
                    toAdd = queue.poll();
                }
                FullUniqueIndex set = null;
                ReferenceListener toRemove = removeQueue.poll();
                while(toRemove != null)
                {
                    if (set == null)
                    {
                        set = new FullUniqueIndex("", IdentityExtractor.getArrayInstance());
                    }
                    set.put(toRemove);
                    toRemove = removeQueue.poll();
                }
                for(int i=0;i<listeners.size();i++)
                {
                    WeakReferenceListener listener = listeners.get(i);
                    ReferenceListener refListener = listener.get();
                    if (refListener == null || (set != null && set.contains(refListener)))
                    {
                        this.listeners.removeByReplacingFromEnd(i);
                        i--;
                    }
                    else
                    {
                        refListener.evictCollectedReferences();
                    }
                }
                synchronized (this)
                {
                    try
                    {
                        this.wait(60000);
                    }
                    catch (InterruptedException e)
                    {
                        //ignore
                    }
                }
            }
            catch (Exception e)
            {
                logger.error("Reference thread error", e);
            }
        }
    }

    public synchronized void runNow()
    {
        this.notify();
    }

    public void removeListener(ReferenceListener listener)
    {
        removeQueue.add(listener);
    }

    private static class WeakReferenceListener extends WeakReference<ReferenceListener>
    {
        private WeakReferenceListener(ReferenceListener referent)
        {
            super(referent);
        }
    }
}
