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

package com.gs.fw.common.mithra.cache.offheap;

import com.gs.fw.common.mithra.cache.AbstractDatedCache;
import com.gs.fw.common.mithra.finder.RelatedFinder;
import com.gs.fw.common.mithra.util.MithraUnsafe;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Arrays;

/**
 * JDK 21 compatible implementation of FastUnsafeOffHeapDataStorage that uses ByteBuffer
 * instead of sun.misc.Unsafe for memory operations.
 */
public class FastUnsafeOffHeapDataStorageJdk21 extends FastUnsafeOffHeapDataStorage
{
    private static final Logger logger = LoggerFactory.getLogger(FastUnsafeOffHeapDataStorageJdk21.class);
    
    /**
     * Creates a new FastUnsafeOffHeapDataStorageJdk21 instance.
     * 
     * @param dataSize the size of each data element
     * @param businessClassName the business class name
     * @param finder the related finder
     */
    public FastUnsafeOffHeapDataStorageJdk21(int dataSize, String businessClassName, RelatedFinder finder)
    {
        super(dataSize, businessClassName, finder);
    }
    
    /**
     * Initializes the storage with ByteBuffer instead of Unsafe.
     * 
     * @param dataSize the size of each data element
     * @param totalAllocated the total allocated size
     * @return a ByteBuffer for the allocated memory
     */
    @Override
    protected Object initializeStorage(int dataSize, long totalAllocated)
    {
        ByteBuffer buffer = ByteBuffer.allocateDirect((int)totalAllocated).order(ByteOrder.nativeOrder());
        
        // Initialize memory
        for (int i = 0; i < totalAllocated; i++) {
            buffer.put(i, (byte) 0);
        }
        
        // Mark first two entries as removed
        buffer.put(0, AbstractDatedCache.REMOVED_VERSION);
        buffer.put(dataSize, AbstractDatedCache.REMOVED_VERSION);
        
        return buffer;
    }
    
    /**
     * Reallocates memory with a new size using ByteBuffer.
     * 
     * @param newSize the new size
     */
    @Override
    protected void reallocateStorage(long newSize)
    {
        ByteBuffer oldBuffer = (ByteBuffer)getBaseAddress();
        ByteBuffer newBuffer = ByteBuffer.allocateDirect((int)newSize).order(ByteOrder.nativeOrder());
        
        // Copy existing memory
        int bytesToCopy = Math.min(oldBuffer.capacity(), newBuffer.capacity());
        for (int i = 0; i < bytesToCopy; i++) {
            newBuffer.put(i, oldBuffer.get(i));
        }
        
        // Zero out new memory
        for (int i = bytesToCopy; i < newSize; i++) {
            newBuffer.put(i, (byte)0);
        }
        
        setBaseAddress(newBuffer);
    }
    
    /**
     * Gets an int value from memory.
     * 
     * @param dataOffset the data offset
     * @param fieldOffset the field offset
     * @return the int value
     */
    @Override
    public int getInt(int dataOffset, int fieldOffset)
    {
        ByteBuffer buffer = (ByteBuffer)getBaseAddress();
        int position = (int)(dataOffset * getDataSize() + fieldOffset);
        return buffer.getInt(position);
    }
    
    /**
     * Gets a boolean value from memory.
     * 
     * @param dataOffset the data offset
     * @param fieldOffset the field offset
     * @return the boolean value
     */
    @Override
    public boolean getBoolean(int dataOffset, int fieldOffset)
    {
        ByteBuffer buffer = (ByteBuffer)getBaseAddress();
        int position = (int)(dataOffset * getDataSize() + fieldOffset);
        return buffer.get(position) == 1;
    }
    
    /**
     * Gets a short value from memory.
     * 
     * @param dataOffset the data offset
     * @param fieldOffset the field offset
     * @return the short value
     */
    @Override
    public short getShort(int dataOffset, int fieldOffset)
    {
        ByteBuffer buffer = (ByteBuffer)getBaseAddress();
        int position = (int)(dataOffset * getDataSize() + fieldOffset);
        return buffer.getShort(position);
    }
    
    /**
     * Gets a char value from memory.
     * 
     * @param dataOffset the data offset
     * @param fieldOffset the field offset
     * @return the char value
     */
    @Override
    public char getChar(int dataOffset, int fieldOffset)
    {
        ByteBuffer buffer = (ByteBuffer)getBaseAddress();
        int position = (int)(dataOffset * getDataSize() + fieldOffset);
        return buffer.getChar(position);
    }
    
    /**
     * Gets a byte value from memory.
     * 
     * @param dataOffset the data offset
     * @param fieldOffset the field offset
     * @return the byte value
     */
    @Override
    public byte getByte(int dataOffset, int fieldOffset)
    {
        ByteBuffer buffer = (ByteBuffer)getBaseAddress();
        int position = (int)(dataOffset * getDataSize() + fieldOffset);
        return buffer.get(position);
    }
    
    /**
     * Gets a long value from memory.
     * 
     * @param dataOffset the data offset
     * @param fieldOffset the field offset
     * @return the long value
     */
    @Override
    public long getLong(int dataOffset, int fieldOffset)
    {
        ByteBuffer buffer = (ByteBuffer)getBaseAddress();
        int position = (int)(dataOffset * getDataSize() + fieldOffset);
        return buffer.getLong(position);
    }
    
    /**
     * Gets a float value from memory.
     * 
     * @param dataOffset the data offset
     * @param fieldOffset the field offset
     * @return the float value
     */
    @Override
    public float getFloat(int dataOffset, int fieldOffset)
    {
        ByteBuffer buffer = (ByteBuffer)getBaseAddress();
        int position = (int)(dataOffset * getDataSize() + fieldOffset);
        return buffer.getFloat(position);
    }
    
    /**
     * Gets a double value from memory.
     * 
     * @param dataOffset the data offset
     * @param fieldOffset the field offset
     * @return the double value
     */
    @Override
    public double getDouble(int dataOffset, int fieldOffset)
    {
        ByteBuffer buffer = (ByteBuffer)getBaseAddress();
        int position = (int)(dataOffset * getDataSize() + fieldOffset);
        return buffer.getDouble(position);
    }
    
    /**
     * Sets a boolean value in memory.
     * 
     * @param dataOffset the data offset
     * @param fieldOffset the field offset
     * @param value the boolean value
     */
    @Override
    public void setBoolean(int dataOffset, int fieldOffset, boolean value)
    {
        ByteBuffer buffer = (ByteBuffer)getBaseAddress();
        int position = (int)(dataOffset * getDataSize() + fieldOffset);
        buffer.put(position, value ? (byte)1 : (byte)0);
    }
    
    /**
     * Sets an int value in memory.
     * 
     * @param dataOffset the data offset
     * @param fieldOffset the field offset
     * @param value the int value
     */
    @Override
    public void setInt(int dataOffset, int fieldOffset, int value)
    {
        ByteBuffer buffer = (ByteBuffer)getBaseAddress();
        int position = (int)(dataOffset * getDataSize() + fieldOffset);
        buffer.putInt(position, value);
    }
    
    /**
     * Sets a short value in memory.
     * 
     * @param dataOffset the data offset
     * @param fieldOffset the field offset
     * @param value the short value
     */
    @Override
    public void setShort(int dataOffset, int fieldOffset, short value)
    {
        ByteBuffer buffer = (ByteBuffer)getBaseAddress();
        int position = (int)(dataOffset * getDataSize() + fieldOffset);
        buffer.putShort(position, value);
    }
    
    /**
     * Sets a char value in memory.
     * 
     * @param dataOffset the data offset
     * @param fieldOffset the field offset
     * @param value the char value
     */
    @Override
    public void setChar(int dataOffset, int fieldOffset, char value)
    {
        ByteBuffer buffer = (ByteBuffer)getBaseAddress();
        int position = (int)(dataOffset * getDataSize() + fieldOffset);
        buffer.putChar(position, value);
    }
    
    /**
     * Sets a byte value in memory.
     * 
     * @param dataOffset the data offset
     * @param fieldOffset the field offset
     * @param value the byte value
     */
    @Override
    public void setByte(int dataOffset, int fieldOffset, byte value)
    {
        ByteBuffer buffer = (ByteBuffer)getBaseAddress();
        int position = (int)(dataOffset * getDataSize() + fieldOffset);
        buffer.put(position, value);
    }
    
    /**
     * Sets a long value in memory.
     * 
     * @param dataOffset the data offset
     * @param fieldOffset the field offset
     * @param value the long value
     */
    @Override
    public void setLong(int dataOffset, int fieldOffset, long value)
    {
        ByteBuffer buffer = (ByteBuffer)getBaseAddress();
        int position = (int)(dataOffset * getDataSize() + fieldOffset);
        buffer.putLong(position, value);
    }
    
    /**
     * Sets a float value in memory.
     * 
     * @param dataOffset the data offset
     * @param fieldOffset the field offset
     * @param value the float value
     */
    @Override
    public void setFloat(int dataOffset, int fieldOffset, float value)
    {
        ByteBuffer buffer = (ByteBuffer)getBaseAddress();
        int position = (int)(dataOffset * getDataSize() + fieldOffset);
        buffer.putFloat(position, value);
    }
    
    /**
     * Sets a double value in memory.
     * 
     * @param dataOffset the data offset
     * @param fieldOffset the field offset
     * @param value the double value
     */
    @Override
    public void setDouble(int dataOffset, int fieldOffset, double value)
    {
        ByteBuffer buffer = (ByteBuffer)getBaseAddress();
        int position = (int)(dataOffset * getDataSize() + fieldOffset);
        buffer.putDouble(position, value);
    }
}
