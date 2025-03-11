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

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.lang.reflect.Field;

/**
 * JDK 9+ compatible implementation of Unsafe operations using VarHandle.
 * This class provides similar functionality to sun.misc.Unsafe but uses
 * the supported VarHandle API introduced in JDK 9.
 */
public class VarHandleUnsafe
{
    /**
     * Gets a VarHandle for a field.
     * 
     * @param field the field to get a VarHandle for
     * @return a VarHandle for the field
     * @throws IllegalAccessException if access is denied
     */
    public static VarHandle getVarHandleForField(Field field) throws IllegalAccessException
    {
        return MethodHandles.privateLookupIn(field.getDeclaringClass(), MethodHandles.lookup())
                .unreflectVarHandle(field);
    }
    
    /**
     * Gets the offset of a field.
     * This is a compatibility method that returns a dummy value since
     * VarHandle doesn't use offsets.
     * 
     * @param field the field to get the offset for
     * @return a dummy offset value
     */
    public static long objectFieldOffset(Field field)
    {
        // VarHandle doesn't use offsets, but we return a dummy value for compatibility
        return 1L;
    }
    
    /**
     * Performs a compare-and-set operation on an int field.
     * 
     * @param handle the VarHandle for the field
     * @param obj the object containing the field
     * @param expect the expected value
     * @param update the new value
     * @return true if successful, false otherwise
     */
    public static boolean compareAndSetInt(VarHandle handle, Object obj, int expect, int update)
    {
        return handle.compareAndSet(obj, expect, update);
    }
    
    /**
     * Performs a compare-and-set operation on a long field.
     * 
     * @param handle the VarHandle for the field
     * @param obj the object containing the field
     * @param expect the expected value
     * @param update the new value
     * @return true if successful, false otherwise
     */
    public static boolean compareAndSetLong(VarHandle handle, Object obj, long expect, long update)
    {
        return handle.compareAndSet(obj, expect, update);
    }
    
    /**
     * Gets an int value from a field.
     * 
     * @param handle the VarHandle for the field
     * @param obj the object containing the field
     * @return the int value
     */
    public static int getInt(VarHandle handle, Object obj)
    {
        return (int) handle.get(obj);
    }
    
    /**
     * Gets a long value from a field.
     * 
     * @param handle the VarHandle for the field
     * @param obj the object containing the field
     * @return the long value
     */
    public static long getLong(VarHandle handle, Object obj)
    {
        return (long) handle.get(obj);
    }
    
    /**
     * Sets an int value in a field.
     * 
     * @param handle the VarHandle for the field
     * @param obj the object containing the field
     * @param value the int value to set
     */
    public static void putInt(VarHandle handle, Object obj, int value)
    {
        handle.set(obj, value);
    }
    
    /**
     * Sets a long value in a field.
     * 
     * @param handle the VarHandle for the field
     * @param obj the object containing the field
     * @param value the long value to set
     */
    public static void putLong(VarHandle handle, Object obj, long value)
    {
        handle.set(obj, value);
    }
}
