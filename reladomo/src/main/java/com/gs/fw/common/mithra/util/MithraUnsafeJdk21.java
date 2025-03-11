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

import java.lang.invoke.VarHandle;
import java.lang.reflect.Field;

/**
 * JDK 21 compatible implementation of MithraUnsafe that uses VarHandle instead of sun.misc.Unsafe.
 */
public class MithraUnsafeJdk21
{
    /**
     * Gets a VarHandle for a field.
     * 
     * @param clazz the class containing the field
     * @param fieldName the name of the field
     * @return a VarHandle for the field
     */
    public static VarHandle getVarHandleForField(Class<?> clazz, String fieldName)
    {
        try {
            Field field = clazz.getDeclaredField(fieldName);
            field.setAccessible(true);
            return VarHandleUnsafe.getVarHandleForField(field);
        } catch (Exception e) {
            throw new RuntimeException("Could not get VarHandle for field " + fieldName, e);
        }
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
        return VarHandleUnsafe.compareAndSetInt(handle, obj, expect, update);
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
        return VarHandleUnsafe.compareAndSetLong(handle, obj, expect, update);
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
        return VarHandleUnsafe.getInt(handle, obj);
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
        return VarHandleUnsafe.getLong(handle, obj);
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
        VarHandleUnsafe.putInt(handle, obj, value);
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
        VarHandleUnsafe.putLong(handle, obj, value);
    }
}
