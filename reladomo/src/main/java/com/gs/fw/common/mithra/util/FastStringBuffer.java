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



/**
 * This is a non-synchronized implementation of string buffer, which
 * offers better performance than the class java.lang.StringBuffer.
 */
public class FastStringBuffer
{
    private int length;
    private char buffer[];

    public FastStringBuffer()
    {
        this(80);
    }

    public FastStringBuffer(int len)
    {
        this.buffer = new char[len];
    }

    public void resetLength()
    {
        this.length = 0;
    }

    public void ensureCapacity(int minStorage)
    {
        int newStorage = (this.buffer.length * 2) + 5;
        if (newStorage < minStorage)
        {
            newStorage = minStorage;
        }
        char newBuf[] = new char[newStorage];
        System.arraycopy(this.buffer, 0, newBuf, 0, this.length);
        this.buffer = newBuf;
    }

    public FastStringBuffer append(String str)
    {
        int oldlen = str.length();
        int newlen = this.length + oldlen;
        if (newlen > this.buffer.length)
        {
            ensureCapacity(newlen);
        }
        str.getChars(0, oldlen, this.buffer, this.length);
        this.length = newlen;
        return this;
    }

    public FastStringBuffer append(String str, int maxLength)
    {
        int oldlen = maxLength;
        int newlen = this.length + oldlen;
        if (newlen > this.buffer.length)
        {
            ensureCapacity(newlen);
        }
        str.getChars(0, oldlen, this.buffer, this.length);
        this.length = newlen;
        return this;
    }

    public FastStringBuffer append(char c)
    {
        if (this.length+1 > this.buffer.length)
        {
            ensureCapacity(this.length + 1);
        }
        this.buffer[this.length] = c;
        this.length++;
        return this;
    }

    public String toString()
    {
        return new String(this.buffer, 0, this.length);
    }

    public int length()
    {
        return length;
    }

    public char charAt(int i)
    {
        return this.buffer[i];
    }

    public boolean startsWith(CharSequence toMatch)
    {
        int len = toMatch.length();
        if (this.length >= len)
        {
            for(int i=0;i<len;i++)
            {
                if (this.buffer[i] != toMatch.charAt(i)) return false;
            }
            return true;
        }
        return false;
    }
}
