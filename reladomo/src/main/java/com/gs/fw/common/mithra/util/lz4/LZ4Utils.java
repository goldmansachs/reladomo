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

enum LZ4Utils
{
    ;

    static final int MEMORY_USAGE = 14;

    static final int MIN_MATCH = 4;

    static final int COPY_LENGTH = 8;
    static final int LAST_LITERALS = 5;
    static final int MF_LIMIT = COPY_LENGTH + MIN_MATCH;
    static final int MIN_LENGTH = MF_LIMIT + 1;

    static final int ML_BITS = 4;
    static final int ML_MASK = (1 << ML_BITS) - 1;
    static final int RUN_BITS = 8 - ML_BITS;
    static final int RUN_MASK = (1 << RUN_BITS) - 1;


    static void safeIncrementalCopy(byte[] dest, int matchOff, int dOff, int matchLen)
    {
        for (int i = 0; i < matchLen; ++i)
        {
            dest[dOff + i] = dest[matchOff + i];
        }
    }

    static void wildIncrementalCopy(byte[] dest, int matchOff, int dOff, int matchCopyEnd)
    {
        do
        {
            copy8Bytes(dest, matchOff, dest, dOff);
            matchOff += 8;
            dOff += 8;
        } while (dOff < matchCopyEnd);
    }

    static void copy8Bytes(byte[] src, int sOff, byte[] dest, int dOff)
    {
        for (int i = 0; i < 8; ++i)
        {
            dest[dOff + i] = src[sOff + i];
        }
    }

    static int commonBytes(byte[] b, int o1, int o2, int limit)
    {
        int count = 0;
        while (o2 < limit && b[o1++] == b[o2++])
        {
            ++count;
        }
        return count;
    }

    static int commonBytesBackward(byte[] b, int o1, int o2, int l1, int l2)
    {
        int count = 0;
        while (o1 > l1 && o2 > l2 && b[--o1] == b[--o2])
        {
            ++count;
        }
        return count;
    }

    static void safeArraycopy(byte[] src, int sOff, byte[] dest, int dOff, int len)
    {
        System.arraycopy(src, sOff, dest, dOff, len);
    }

    static void wildArraycopy(byte[] src, int sOff, byte[] dest, int dOff, int len)
    {
        for (int i = 0; i < len; i += 8)
        {
            copy8Bytes(src, sOff + i, dest, dOff + i);
        }
    }

    public static int readIntBE(byte[] buf, int i)
    {
        return ((buf[i] & 0xFF) << 24) | ((buf[i + 1] & 0xFF) << 16) | ((buf[i + 2] & 0xFF) << 8) | (buf[i + 3] & 0xFF);
    }

    public static int readIntLE(byte[] buf, int i)
    {
        return (buf[i] & 0xFF) | ((buf[i + 1] & 0xFF) << 8) | ((buf[i + 2] & 0xFF) << 16) | ((buf[i + 3] & 0xFF) << 24);
    }

}
