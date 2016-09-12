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

package com.gs.fw.common.mithra.cache.bean;


import java.util.concurrent.atomic.AtomicReferenceArray;

public class MutableBeanPool<E extends MutableBean>
{
    private static final Object EMPTY = new Object();
    private AtomicReferenceArray array = new AtomicReferenceArray(1024+16);

    private MutableBean.Factory factory;

    public MutableBeanPool(MutableBean.Factory factory)
    {
        this.factory = factory;
        for(int i=0;i<1024+16;i++)
        {
            array.set(i, EMPTY);
        }
    }

    public E getOrConstruct()
    {
        int index = (int) (Thread.currentThread().getId() & 1023);
        index = (index >> 2) + ((index & 3) << 8);
        int end = index + 16;
        while(index < end)
        {
            Object result = array.get(index);
            if (result == EMPTY)
            {
                return construct(index);
            }
            else if (result != null)
            {
                if (array.compareAndSet(index, result, null))
                {
                    return (E) result;
                }
            }
            index++;
        }
        return (E) factory.construct((((index - 16) & 1023) + (int)(Thread.currentThread().getId() & 3)) & 1023);
    }

    private E construct(int index)
    {
        Object result;
        result = factory.construct(index);
        array.compareAndSet(index, EMPTY, null); // it's ok if it fails.
        return (E) result;
    }

    public void release(MutableBean bean)
    {
        array.lazySet(bean.getCachePosition(), bean);
    }
}
