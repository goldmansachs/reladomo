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

import java.util.concurrent.atomic.AtomicInteger;


public class TempTableNamer
{

    private static final AtomicInteger txCount = new AtomicInteger(-1);

    private static final long FIVE_BITS_MASK = (1 << 5) -1;

    private static short pid;

    private static long ip;

    private static final char intToBase32[] = {
        'A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I', 'J', 'K', 'L', 'M',
        'N', 'O', 'P', 'Q', 'R', 'S', 'T', 'U', 'V', 'W', 'X', 'Y', 'Z',
        '0', '1', '2', '3', '4', '5'
    };

    private static volatile char[] constantStart = new char[15];

    static
    {
        ip = MithraProcessInfo.getIpAsLong();
        pid = MithraProcessInfo.getPidAsShort();
        fillConstantStart();
    }

    private static void fillConstantStart()
    {
        long startTime = maskUpperBits(System.currentTimeMillis() >> 4, 45);
        char[] head = new char[15];
        fillBits(head, maskUpperBits(pid, 15) << 15 | maskUpperBits(ip >> (32 - 15), 15) , 0, 15+15);
        fillBits(head, startTime, 6, 45);
        constantStart = head;
    }

    public static long maskUpperBits(long incoming, int lowerBits)
    {
        return incoming & ((1L << lowerBits) - 1);
    }

    public static void fillBits(char[] dest, long src, int start, int bitsToFill)
    {
        while(bitsToFill != 0)
        {
            bitsToFill -= 5;
            dest[start] = intToBase32[(int)(src & FIVE_BITS_MASK)];
            src >>>= 5;
            start++;
        }
    }

    public static String getNextTempTableName()
    {
        char[] result = new char[19];
        long next = maskUpperBits(txCount.incrementAndGet(), 20);
        fillBits(result, next, 0, 20);
        if (next == 500000)
        {
            fillConstantStart();
        }
        System.arraycopy(constantStart, 0, result, 4, 15);
        return new String(result);
    }

    public static void main(String[] args)
    {
        int result = 0;
        int pos = 0;
        for(int i=0;i<args[0].length();i++)
        {
            result = result | (decode(args[0].charAt(i)) << pos);
            pos += 5;
            if (pos == 20)
            {
                System.out.println(padTo20(Integer.toBinaryString(result)));
                result = 0;
                pos = 0;
            }
        }
        System.out.println(padTo20(Integer.toBinaryString(result)));
    }

    private static int decode(char c)
    {
        for(int i=0;i<intToBase32.length;i++)
        {
            if ( c == intToBase32[i] ) return i;
        }
        throw new RuntimeException("can't decode "+c);
    }

    private static String padTo20(String s)
    {
        String result = s;
        while(result.length() < 20) result = "0" + result;
        return result;
    }

}
