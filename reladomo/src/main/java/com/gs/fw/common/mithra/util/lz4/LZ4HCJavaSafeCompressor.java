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

/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/*
    Copyright Adrien Grand (based on Yann Collet's BSD licensed LZ4 implementation)
    changes copyright Goldman Sachs, licensed under Apache 2.0 license
*/
package com.gs.fw.common.mithra.util.lz4;

import java.nio.ByteOrder;
import java.util.Arrays;

import static com.gs.fw.common.mithra.util.lz4.LZ4Utils.*;

/**
 * High compression compressor written in pure Java without using the unofficial
 * sun.misc.Unsafe API.
 */
public class LZ4HCJavaSafeCompressor
{
    private static final ByteOrder NATIVE_BYTE_ORDER = ByteOrder.nativeOrder();
    private static final int OPTIMAL_ML = ML_MASK - 1 + MIN_MATCH;

    private final HashTable ht = new HashTable(0);
    private final Match match0 = new Match();
    private final Match match1 = new Match();
    private final Match match2 = new Match();
    private final Match match3 = new Match();

    public final int maxCompressedLength(int length)
    {
        if (length < 0)
        {
            throw new IllegalArgumentException("length must be >= 0, got " + length);
        }
        return length + length / 255 + 16;
    }

    public final int compress(byte[] src, int srcOff, int srcLen, byte[] dest, int destOff)
    {
        return compress(src, srcOff, srcLen, dest, destOff, dest.length - destOff);
    }


    static void copyTo(Match m1, Match m2)
    {
        m2.len = m1.len;
        m2.start = m1.start;
        m2.ref = m1.ref;
    }

    public int compress(byte[] src, int srcOff, int srcLen, byte[] dest,
            int destOff, int maxDestLen)
    {

        final int srcEnd = srcOff + srcLen;
        final int destEnd = destOff + maxDestLen;
        final int mfLimit = srcEnd - MF_LIMIT;
        final int matchLimit = srcEnd - LAST_LITERALS;

        int sOff = srcOff;
        int dOff = destOff;
        int anchor = sOff++;

        ht.reset(srcOff);
        match0.reset();
        match1.reset();
        match2.reset();
        match3.reset();

        main:
        while (sOff < mfLimit)
        {
            if (!ht.insertAndFindBestMatch(src, sOff, matchLimit, match1))
            {
                ++sOff;
                continue;
            }

            // saved, in case we would skip too much
            copyTo(match1, match0);

            search2:
            while (true)
            {
                assert match1.start >= anchor;
                if (match1.end() >= mfLimit
                        || !ht.insertAndFindWiderMatch(src, match1.end() - 2, match1.start + 1, matchLimit, match1.len, match2))
                {
                    // no better match
                    dOff = encodeSequence(src, anchor, match1.start, match1.ref, match1.len, dest, dOff, destEnd);
                    anchor = sOff = match1.end();
                    continue main;
                }

                if (match0.start < match1.start)
                {
                    if (match2.start < match1.start + match0.len)
                    { // empirical
                        copyTo(match0, match1);
                    }
                }
                assert match2.start > match1.start;

                if (match2.start - match1.start < 3)
                { // First Match too small : removed
                    copyTo(match2, match1);
                    continue search2;
                }

                search3:
                while (true)
                {
                    if (match2.start - match1.start < OPTIMAL_ML)
                    {
                        int newMatchLen = match1.len;
                        if (newMatchLen > OPTIMAL_ML)
                        {
                            newMatchLen = OPTIMAL_ML;
                        }
                        if (match1.start + newMatchLen > match2.end() - MIN_MATCH)
                        {
                            newMatchLen = match2.start - match1.start + match2.len - MIN_MATCH;
                        }
                        final int correction = newMatchLen - (match2.start - match1.start);
                        if (correction > 0)
                        {
                            match2.fix(correction);
                        }
                    }

                    if (match2.start + match2.len >= mfLimit
                            || !ht.insertAndFindWiderMatch(src, match2.end() - 3, match2.start, matchLimit, match2.len, match3))
                    {
                        // no better match -> 2 sequences to encode
                        if (match2.start < match1.end())
                        {
                            match1.len = match2.start - match1.start;
                        }
                        // encode seq 1
                        dOff = encodeSequence(src, anchor, match1.start, match1.ref, match1.len, dest, dOff, destEnd);
                        anchor = sOff = match1.end();
                        // encode seq 2
                        dOff = encodeSequence(src, anchor, match2.start, match2.ref, match2.len, dest, dOff, destEnd);
                        anchor = sOff = match2.end();
                        continue main;
                    }

                    if (match3.start < match1.end() + 3)
                    { // Not enough space for match 2 : remove it
                        if (match3.start >= match1.end())
                        { // // can write Seq1 immediately ==> Seq2 is removed, so Seq3 becomes Seq1
                            if (match2.start < match1.end())
                            {
                                final int correction = match1.end() - match2.start;
                                match2.fix(correction);
                                if (match2.len < MIN_MATCH)
                                {
                                    copyTo(match3, match2);
                                }
                            }

                            dOff = encodeSequence(src, anchor, match1.start, match1.ref, match1.len, dest, dOff, destEnd);
                            anchor = sOff = match1.end();

                            copyTo(match3, match1);
                            copyTo(match2, match0);

                            continue search2;
                        }

                        copyTo(match3, match2);
                        continue search3;
                    }

                    // OK, now we have 3 ascending matches; let's write at least the first one
                    if (match2.start < match1.end())
                    {
                        if (match2.start - match1.start < ML_MASK)
                        {
                            if (match1.len > OPTIMAL_ML)
                            {
                                match1.len = OPTIMAL_ML;
                            }
                            if (match1.end() > match2.end() - MIN_MATCH)
                            {
                                match1.len = match2.end() - match1.start - MIN_MATCH;
                            }
                            final int correction = match1.end() - match2.start;
                            match2.fix(correction);
                        }
                        else
                        {
                            match1.len = match2.start - match1.start;
                        }
                    }

                    dOff = encodeSequence(src, anchor, match1.start, match1.ref, match1.len, dest, dOff, destEnd);
                    anchor = sOff = match1.end();

                    copyTo(match2, match1);
                    copyTo(match3, match2);

                    continue search3;
                }

            }

        }

        dOff = lastLiterals(src, anchor, srcEnd - anchor, dest, dOff, destEnd);
        return dOff - destOff;
    }

    static int encodeSequence(byte[] src, int anchor, int matchOff, int matchRef, int matchLen, byte[] dest, int dOff, int destEnd)
    {
        final int runLen = matchOff - anchor;
        final int tokenOff = dOff++;

        if (dOff + runLen + (2 + 1 + LAST_LITERALS) + (runLen >>> 8) > destEnd)
        {
            throw new RuntimeException("maxDestLen is too small");
        }

        int token;
        if (runLen >= RUN_MASK)
        {
            token = (byte) (RUN_MASK << ML_BITS);
            dOff = writeLen(runLen - RUN_MASK, dest, dOff);
        }
        else
        {
            token = runLen << ML_BITS;
        }

        // copy literals
        wildArraycopy(src, anchor, dest, dOff, runLen);
        dOff += runLen;

        // encode offset
        final int matchDec = matchOff - matchRef;
        dest[dOff++] = (byte) matchDec;
        dest[dOff++] = (byte) (matchDec >>> 8);

        // encode match len
        matchLen -= 4;
        if (dOff + (1 + LAST_LITERALS) + (matchLen >>> 8) > destEnd)
        {
            throw new RuntimeException("maxDestLen is too small");
        }
        if (matchLen >= ML_MASK)
        {
            token |= ML_MASK;
            dOff = writeLen(matchLen - RUN_MASK, dest, dOff);
        }
        else
        {
            token |= matchLen;
        }

        dest[tokenOff] = (byte) token;

        return dOff;
    }

    static int lastLiterals(byte[] src, int sOff, int srcLen, byte[] dest, int dOff, int destEnd)
    {
        final int runLen = srcLen;

        if (dOff + runLen + 1 + (runLen + 255 - RUN_MASK) / 255 > destEnd)
        {
            throw new RuntimeException("maxDestLen is too small");
        }

        if (runLen >= RUN_MASK)
        {
            dest[dOff++] = (byte) (RUN_MASK << ML_BITS);
            dOff = writeLen(runLen - RUN_MASK, dest, dOff);
        }
        else
        {
            dest[dOff++] = (byte) (runLen << ML_BITS);
        }
        // copy literals
        System.arraycopy(src, sOff, dest, dOff, runLen);
        dOff += runLen;

        return dOff;
    }

    static int writeLen(int len, byte[] dest, int dOff)
    {
        while (len >= 0xFF)
        {
            dest[dOff++] = (byte) 0xFF;
            len -= 0xFF;
        }
        dest[dOff++] = (byte) len;
        return dOff;
    }

    static boolean readIntEquals(byte[] buf, int i, int j)
    {
        return buf[i] == buf[j] && buf[i + 1] == buf[j + 1] && buf[i + 2] == buf[j + 2] && buf[i + 3] == buf[j + 3];
    }

    public static int readInt(byte[] buf, int i)
    {
        if (NATIVE_BYTE_ORDER == ByteOrder.BIG_ENDIAN)
        {
            return readIntBE(buf, i);
        }
        else
        {
            return readIntLE(buf, i);
        }
    }

    class HashTable
    {
        static final int HASH_LOG_HC = 15;
        static final int HASH_TABLE_SIZE_HC = 1 << HASH_LOG_HC;
        static final int MAX_DISTANCE = 1 << 16;


//        static final int MAX_ATTEMPTS = 256;
        static final int MAX_ATTEMPTS = 64;
        static final int MASK = MAX_DISTANCE - 1;
        int nextToUpdate;
        private int base;
        private final int[] hashTable;
        private final short[] chainTable;

        HashTable(int base)
        {
            this.base = base;
            nextToUpdate = base;
            hashTable = new int[HASH_TABLE_SIZE_HC];
            Arrays.fill(hashTable, -1);
            chainTable = new short[MAX_DISTANCE];
        }

        private int hashHC(int i)
        {
            return (i * -1640531535) >>> ((MIN_MATCH * 8) - HASH_LOG_HC);
        }

        public void reset(int base)
        {
            this.base = base;
            this.nextToUpdate = base;
            Arrays.fill(hashTable, -1);
            Arrays.fill(chainTable, (short) 0);
        }

        private int hashPointer(byte[] bytes, int off)
        {
            final int v = readInt(bytes, off);
            final int h = hashHC(v);
            return base + hashTable[h];
        }

        private int next(int off)
        {
            return base + off - (chainTable[off & MASK] & 0xFFFF);
        }

        private void addHash(byte[] bytes, int off)
        {
            final int v = readInt(bytes, off);
            final int h = hashHC(v);
            int delta = off - hashTable[h];
            if (delta >= MAX_DISTANCE)
            {
                delta = MAX_DISTANCE - 1;
            }
            chainTable[off & MASK] = (short) delta;
            hashTable[h] = off - base;
        }

        void insert(int off, byte[] bytes)
        {
            for (; nextToUpdate < off; ++nextToUpdate)
            {
                addHash(bytes, nextToUpdate);
            }
        }

        boolean insertAndFindBestMatch(byte[] buf, int off, int matchLimit, Match match)
        {
            match.start = off;
            match.len = 0;

            insert(off, buf);

            int ref = hashPointer(buf, off);

            if (ref >= off - 4 && ref >= base)
            { // potential repetition
                if (readIntEquals(buf, ref, off))
                { // confirmed
                    final int delta = off - ref;
                    int ptr = off;
                    match.len = MIN_MATCH + commonBytes(buf, ref + MIN_MATCH, off + MIN_MATCH, matchLimit);
                    final int end = off + match.len - (MIN_MATCH - 1);
                    while (ptr < end - delta)
                    {
                        chainTable[ptr & MASK] = (short) delta; // pre load
                        ++ptr;
                    }
                    do
                    {
                        chainTable[ptr & MASK] = (short) delta;
                        hashTable[hashHC(readInt(buf, ptr))] = ptr - base; // head of table
                        ++ptr;
                    } while (ptr < end);
                    nextToUpdate = end;
                    match.ref = ref;
                }
                ref = next(ref);
            }

            for (int i = 0; i < MAX_ATTEMPTS; ++i)
            {
                if (ref < Math.max(base, off - MAX_DISTANCE + 1))
                {
                    break;
                }
                if (buf[ref + match.len] == buf[off + match.len] && readIntEquals(buf, ref, off))
                {
                    final int matchLen = MIN_MATCH + commonBytes(buf, ref + MIN_MATCH, off + MIN_MATCH, matchLimit);
                    if (matchLen > match.len)
                    {
                        match.ref = ref;
                        match.len = matchLen;
                    }
                }
                ref = next(ref);
            }

            return match.len != 0;
        }

        boolean insertAndFindWiderMatch(byte[] buf, int off, int startLimit, int matchLimit, int minLen, Match match)
        {
            match.len = minLen;

            insert(off, buf);

            final int delta = off - startLimit;
            int ref = hashPointer(buf, off);
            for (int i = 0; i < MAX_ATTEMPTS; ++i)
            {
                if (ref < Math.max(base, off - MAX_DISTANCE + 1))
                {
                    break;
                }
                if (buf[ref - delta + match.len] == buf[startLimit + match.len]
                        && readIntEquals(buf, ref, off))
                {
                    final int matchLenForward = MIN_MATCH + commonBytes(buf, ref + MIN_MATCH, off + MIN_MATCH, matchLimit);
                    final int matchLenBackward = commonBytesBackward(buf, ref, off, base, startLimit);
                    final int matchLen = matchLenBackward + matchLenForward;
                    if (matchLen > match.len)
                    {
                        match.len = matchLen;
                        match.ref = ref - matchLenBackward;
                        match.start = off - matchLenBackward;
                    }
                }
                ref = next(ref);
            }

            return match.len > minLen;
        }

    }

    static class Match
    {
        int start, ref, len;

        void fix(int correction)
        {
            start += correction;
            ref += correction;
            len -= correction;
        }

        int end()
        {
            return start + len;
        }

        public void reset()
        {
            this.start = ref = len = 0;
        }
    }
}
