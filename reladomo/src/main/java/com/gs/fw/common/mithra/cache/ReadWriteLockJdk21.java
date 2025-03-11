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

import com.gs.fw.common.mithra.util.VarHandleUnsafe;

import java.lang.invoke.VarHandle;
import java.lang.reflect.Field;

/**
 * JDK 21 compatible implementation of ReadWriteLock that uses VarHandle instead of sun.misc.Unsafe.
 */
public class ReadWriteLockJdk21 extends ReadWriteLock
{
    private static final VarHandle STATE_HANDLE;
    
    static
    {
        try {
            Field stateField = ReadWriteLock.class.getDeclaredField("state");
            stateField.setAccessible(true);
            STATE_HANDLE = VarHandleUnsafe.getVarHandleForField(stateField);
        } catch (Exception e) {
            throw new RuntimeException("Could not initialize ReadWriteLockJdk21", e);
        }
    }
    
    /**
     * Acquires a read lock.
     */
    @Override
    public void acquireReadLock()
    {
        int s;
        do
        {
            while ((s = (int) STATE_HANDLE.get(this)) < 0)
            {
                Thread.yield();
            }
        }
        while (!STATE_HANDLE.compareAndSet(this, s, s + 1));
    }
    
    /**
     * Releases a read lock.
     */
    @Override
    public void releaseReadLock()
    {
        int s;
        do
        {
            s = (int) STATE_HANDLE.get(this);
        }
        while (!STATE_HANDLE.compareAndSet(this, s, s - 1));
    }
    
    /**
     * Acquires a write lock.
     */
    @Override
    public void acquireWriteLock()
    {
        while (!STATE_HANDLE.compareAndSet(this, 0, -1))
        {
            Thread.yield();
        }
    }
    
    /**
     * Releases a write lock.
     */
    @Override
    public void releaseWriteLock()
    {
        STATE_HANDLE.set(this, 0);
    }
    
    /**
     * Upgrades a read lock to a write lock.
     * 
     * @return true if the upgrade was successful, false otherwise
     */
    @Override
    public boolean upgradeToWriteLock()
    {
        int s;
        do
        {
            s = (int) STATE_HANDLE.get(this);
            if (s != 1) return false;
        }
        while (!STATE_HANDLE.compareAndSet(this, 1, -1));
        return true;
    }
    
    /**
     * Downgrades a write lock to a read lock.
     */
    @Override
    public void downgradeToReadLock()
    {
        STATE_HANDLE.set(this, 1);
    }
}
