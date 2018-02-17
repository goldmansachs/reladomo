
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
// Portions copyright Hiroshi Ito. Licensed under Apache 2.0 license

package com.gs.fw.common.mithra.finder.bytearray;

import org.eclipse.collections.api.block.HashingStrategy;
import org.eclipse.collections.api.block.procedure.Procedure;
import org.eclipse.collections.impl.set.strategy.mutable.UnifiedSetWithHashingStrategy;

import java.util.Arrays;

public class ByteArraySet extends UnifiedSetWithHashingStrategy<byte[]>
{

    private static final HashingStrategy hasher = new ByteArrayHasher();

    public ByteArraySet(int initialSize)
    {
        super(hasher, initialSize);
    }

    public ByteArraySet()
    {
        super(hasher);
    }

    @Override
    public String toString()
    {
        final StringBuilder builder = new StringBuilder();
        builder.append("[");
        this.forEach(new Procedure<byte[]>()
        {
            public void value(byte[] object)
            {
                builder.append(Arrays.toString(object));
                builder.append(',');
            }
        });
        if (this.size() > 0) builder.deleteCharAt(builder.length() - 1);
        builder.append("]");
        return builder.toString();
    }

    private static class ByteArrayHasher implements HashingStrategy
    {
        public int computeHashCode(Object object)
        {
            return Arrays.hashCode((byte[]) object);
        }

        public boolean equals(Object left, Object right)
        {
            return Arrays.equals(((byte[]) left), (byte[]) right);
        }
    }
}
