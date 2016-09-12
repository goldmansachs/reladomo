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

package com.gs.fw.common.mithra;

import com.gs.fw.common.mithra.finder.RelatedFinder;


/**
 * This class provides a thread local context and a few utility methods
 * @param <T> The Mithra object class that this listener is for
 */
public abstract class MithraUpdateListenerAbstract<T extends MithraTransactionalObject> implements MithraUpdateListener<T>
{
    private static ThreadLocal context = new ThreadLocal();

    public static void setContextData(Object contextData)
    {
        context.set(contextData);
    }

    public Object getContextData()
    {
        return context.get();
    }

    public RelatedFinder getFinderInstance(MithraTransactionalObject object)
    {
        return object.zGetPortal().getFinder();
    }
}
