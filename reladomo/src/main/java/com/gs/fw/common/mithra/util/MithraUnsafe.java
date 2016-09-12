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


import com.gs.fw.common.mithra.attribute.AsOfAttribute;
import sun.misc.Unsafe;

import java.lang.reflect.Field;
import java.security.AccessController;
import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;
import java.util.TreeSet;

public class MithraUnsafe
{
    private static Unsafe UNSAFE = getUnsafe();
    private static AuditedMemory AUDITED_MEMORY;

    public static Unsafe getUnsafe()
    {
        try
        {
            return Unsafe.getUnsafe();
        }
        catch (SecurityException ignored)
        {
            try
            {
                return AccessController.doPrivileged(new PrivilegedExceptionAction<Unsafe>()
                {
                    public Unsafe run() throws Exception
                    {
                        Field f = Unsafe.class.getDeclaredField("theUnsafe");
                        f.setAccessible(true);
                        return (Unsafe) f.get(null);
                    }
                });
            }
            catch (PrivilegedActionException e)
            {
                throw new RuntimeException("Could not initialize intrinsics",
                        e.getCause());
            }
        }
    }

    public synchronized static AuditedMemory getAuditedMemory()
    {
        if (AUDITED_MEMORY == null)
        {
            AUDITED_MEMORY = new AuditedMemory();
        }
        return AUDITED_MEMORY;
    }

    public static long findCurrentDataOffset(AsOfAttribute asOfAttribute)
    {
        String implClassName = asOfAttribute.zGetTopOwnerClassName().replace('/','.');
        Class<?> mapClass;
        try
        {
            mapClass = Class.forName(implClassName);
        }
        catch (ClassNotFoundException e)
        {
            throw new RuntimeException("Could not find currentData member for class "+implClassName, e);
        }
        while(true)
        {
            try
            {
                return UNSAFE.objectFieldOffset(mapClass.getDeclaredField("currentData"));
            }
            catch (NoSuchFieldException e)
            {
                mapClass = mapClass.getSuperclass();
                if (mapClass.equals(Object.class))
                {
                    throw new RuntimeException("Could not find currentData member for class "+implClassName);
                }
            }
        }
    }

    public static class AuditedMemory
    {
        private final TreeSet<MemoryBlock> allocatedMemory = new TreeSet<MemoryBlock>();
        private final MemoryBlock mutableProbe = new MemoryBlock(0,0);

        public long allocateMemory(long totalAllocated)
        {
            long start = UNSAFE.allocateMemory(totalAllocated);
            assert addAllocatedMemory(start, totalAllocated);
            return start;
        }

        private boolean addAllocatedMemory(long start, long totalAllocated)
        {
            synchronized (allocatedMemory)
            {
                allocatedMemory.add(new MemoryBlock(start, totalAllocated));
            }
            return true;
        }

        public void freeMemory(long src)
        {
            assert verifyFreeMemory(src);
            UNSAFE.freeMemory(src);
        }

        public void setMemory(long src, long length, byte value)
        {
            assert verifyAccess(src, length);
            UNSAFE.setMemory(src, length, value);
        }

        public boolean compareAndSwapLong(Object o, long offset, long expected, long newValue)
        {
            return UNSAFE.compareAndSwapLong(o, offset, expected, newValue);
        }

        public long reallocateMemory(long baseAddress, long newSize)
        {
            assert verifyFreeMemory(baseAddress);
            long start = UNSAFE.reallocateMemory(baseAddress, newSize);
            assert addAllocatedMemory(start, newSize);
            return start;
        }

        private boolean verifyFreeMemory(long src)
        {
            synchronized (allocatedMemory)
            {
                mutableProbe.start = src;
                return allocatedMemory.remove(mutableProbe);
            }
        }

        private boolean verifyAccess(long address, long length)
        {
            synchronized (allocatedMemory)
            {
                mutableProbe.start = address;
                MemoryBlock floor = allocatedMemory.floor(mutableProbe);
                assert length > 0;
                assert floor != null;
                assert address >= floor.start;
                assert address + length <= floor.start + floor.lengthInBytes;
                return true;
            }
        }

        public void copyMemory(long src, long dest, long copySize)
        {
            assert verifyAccess(src, copySize);
            assert verifyAccess(dest, copySize);
            UNSAFE.copyMemory(src, dest, copySize);
        }

        public byte getByte(long src)
        {
            assert verifyAccess(src, 1);
            return UNSAFE.getByte(src);
        }

        public void putByte(long dest, byte value)
        {
            assert verifyAccess(dest, 1);
            UNSAFE.putByte(dest, value);
        }

        public void putChar(long src, char c)
        {
            assert verifyAccess(src, 2);
            UNSAFE.putChar(src, c);
        }

        public void putDouble(long src, double v)
        {
            assert verifyAccess(src, 8);
            UNSAFE.putDouble(src, v);
        }

        public void putFloat(long src, float v)
        {
            assert verifyAccess(src, 4);
            UNSAFE.putFloat(src, v);
        }

        public void putInt(long src, int i)
        {
            assert verifyAccess(src, 4);
            UNSAFE.putInt(src, i);
        }

        public void putLong(long src, long l2)
        {
            assert verifyAccess(src, 8);
            UNSAFE.putLong(src, l2);
        }

        public void putShort(long src, short i)
        {
            assert verifyAccess(src, 2);
            UNSAFE.putShort(src, i);
        }

        public long getLong(long src)
        {
            assert verifyAccess(src, 8);
            return UNSAFE.getLong(src);
        }

        public short getShort(long src)
        {
            assert verifyAccess(src, 2);
            return UNSAFE.getShort(src);
        }

        public char getChar(long src)
        {
            assert verifyAccess(src, 2);
            return UNSAFE.getChar(src);
        }

        public double getDouble(long src)
        {
            assert verifyAccess(src, 8);
            return UNSAFE.getDouble(src);
        }

        public float getFloat(long src)
        {
            assert verifyAccess(src, 4);
            return UNSAFE.getFloat(src);
        }

        public int getInt(long src)
        {
            assert verifyAccess(src, 4);
            return UNSAFE.getInt(src);
        }

    }

    private static class MemoryBlock implements Comparable<MemoryBlock>
    {
        private long start;
        private long lengthInBytes;

        private MemoryBlock(long start, long lengthInBytes)
        {
            this.start = start;
            this.lengthInBytes = lengthInBytes;
        }

        @Override
        public int compareTo(MemoryBlock o)
        {
            return this.start < o.start ? -1 : (this.start == o.start ? 0 : 1);
        }

        @Override
        public boolean equals(Object obj)
        {
            return this.start == ((MemoryBlock)obj).start;
        }
    }
}
