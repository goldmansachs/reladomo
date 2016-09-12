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

import java.math.BigDecimal;



public class HashUtil
{

    public static final int FNV_PRIME = 16777619;

    public static final int NULL_HASH = 1048573; // a large prime who's power of 2 remainder varies alot

    /* IMPORTANT Note:
        We don't define methods for short, char, byte and float. We let those become int or double.
        If we ever define these methods,
        then we need additional methods on the Index interface (get(char), get(short), etc). 
    */

    public static final int hash(int i, boolean isNull)
    {
        if (isNull) return NULL_HASH;
        return i;
    }

    //todo: Double.doubleToLongBits has really bad bit patterns.
    // for example, values 100 to 200 are all zero bits in the LSB for at least 8 bits.
    // need to mix the value, but have to keep it compatible with Double.hashcode !!
    public static final int hash(double d, boolean isNull)
    {
        if (isNull) return NULL_HASH;
        long longVal = Double.doubleToLongBits(d);
        return (int)(longVal ^ (longVal >>> 32));
    }

    public static final int hash(float f, boolean isNull)
    {
        if (isNull) return NULL_HASH;
        return Float.floatToIntBits(f);
    }

    public static final int hash(long d, boolean isNull)
    {
        if (isNull) return NULL_HASH;
        return (int)(d ^ (d >>> 32));
    }

    public static final int hash(boolean b, boolean isNull)
    {
        if (isNull) return NULL_HASH;
        return b ? 1231 : 1237;  // to be compatible with java.lang.Boolean
    }

    public static final int hash(int i)
    {
        return i;
    }

    //todo: Double.doubleToLongBits has really bad bit patterns.
    // for example, values 100 to 200 are all zero bits in the LSB for at least 8 bits.
    // need to mix the value, but have to keep it compatible with Double.hashcode !!
    public static final int hash(double d)
    {
        long longVal = Double.doubleToLongBits(d);
        return (int)(longVal ^ (longVal >>> 32));
    }

    public static final int hash(BigDecimal bd)
    {
        if (bd == null) return NULL_HASH;
        return bd.hashCode();
    }

    public static final int hash(float f)
    {
        return Float.floatToIntBits(f);
    }

    public static final int hash(long d)
    {
        return (int)(d ^ (d >>> 32));
    }

    public static final int hash(boolean b)
    {
        return b ? 1231 : 1237;  // to be compatible with java.lang.Boolean
    }

    public static final int combineHashesHsieh(int hash1, int hash2)
    {
        // this is similar to Paul Hsieh's hash from http://www.azillionmonkeys.com/qed/hash.html
        // it doesn't include the avalanche step
        hash1 += (hash2 & 0xffff);
        hash1 = (hash1 << 16) ^ ((hash2 >>> 5) ^ hash1);
        hash1 += hash1 >>> 11;

        return hash1;
    }

    public static final int combineHashes(int hash1, int hash2)
    {
        // this is similar to Paul Hsieh's hash from http://www.azillionmonkeys.com/qed/hash.html
        // it doesn't include the avalanche step
        hash1 += (hash2 & 0xffff);
        hash1 = (hash1 << 16) ^ ((hash2 >>> 5) ^ hash1);
        hash1 += hash1 >>> 11;

        return hash1;
    }

    public static final int combineHashesMurmur(int hash2, int hash1)
    {
        // this is similar to MurmurHash 3 https://code.google.com/p/smhasher/source/browse/trunk/MurmurHash3.cpp?spec=svn136&r=136
        // it doesn't include the avalanche step, which is probably why it does so badly in uniqueness tests of the lower bits (see TestPerformance)
        hash1 *= 0xcc9e2d51;
        hash1 = Integer.rotateLeft(hash1,15);
        hash1 *= 0x1b873593;

        hash2 ^= hash1;
        hash2 = Integer.rotateLeft(hash2,13);
        hash2 = hash2*5+0xe6546b64;
        return hash2;
    }

    public static final int combineHashesOld(int hash1, int hash2)
    {
        /* similar to the FNV algorithm, not the same, because java doesn't have unsigned int */
        hash1 ^= (hash2 & 0xff);
        hash1 *= FNV_PRIME;
        hash1 ^= (hash2 >>> 8) & 0xff;
        hash1 *= FNV_PRIME;
        hash1 ^= (hash2 >>> 16) & 0xff;
        hash1 *= FNV_PRIME;
        hash1 ^= (hash2 >>> 24) & 0xff;
        hash1 *= FNV_PRIME;
        return hash1;
    }

    public static final int combineHashesBad(int hash1, int hash2)
    {
        return hash1 ^ hash2;
    }

    public static final int hash(byte[] bytes)
    {
        int len = bytes.length;
        int hash = 0;
        for (int i = 0; i < len; i++)
        {
            hash = 29*hash + bytes[i];
        }
        return hash;
    }

    public static final int hash(Object o)
    {
        if (o == null) return NULL_HASH;
        return o.hashCode();
    }

    public static final int offHeapHash(String s)
    {
        return offHeapHash(StringPool.getInstance().getOffHeapAddressWithoutAdding(s));
    }

    public static final int offHeapHash(int stringAsInt)
    {
        return stringAsInt;
    }
}
