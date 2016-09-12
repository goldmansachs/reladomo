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

package com.gs.fw.common.mithra.cache;

import java.lang.ref.WeakReference;
import java.util.concurrent.LinkedBlockingQueue;


public class MithraConcurrentEvictorThread extends Thread
{

    private static final MithraConcurrentEvictorThread instance = new MithraConcurrentEvictorThread();

    private final LinkedBlockingQueue<WeakReferenceListener> queue = new LinkedBlockingQueue<WeakReferenceListener>();

    static
    {
        instance.start();
    }

    public MithraConcurrentEvictorThread()
    {
        super("MithraReferenceEvictorThread");
        this.setDaemon(true);
    }

    public static MithraConcurrentEvictorThread getInstance()
    {
        return instance;
    }

    public void queueEviction(Evictable evictable)
    {
        queue.add(new WeakReferenceListener(evictable));
    }

    @Override
    public void run()
    {
        while(true)
        {
            try
            {
                Evictable toAdd = queue.take().get();
                if (toAdd != null)
                {
                    toAdd.evictCollectedReferences();
                }
            }
            catch (InterruptedException e)
            {
                //ignore
            }
        }
    }

    private static class WeakReferenceListener extends WeakReference<Evictable>
    {
        private WeakReferenceListener(Evictable referent)
        {
            super(referent);
        }
    }
}