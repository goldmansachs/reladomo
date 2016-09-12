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

package com.gs.fw.common.mithra.transaction;

import com.gs.fw.common.mithra.MithraTransaction;

import java.util.concurrent.atomic.AtomicInteger;


public class TransactionLocal
{

    public final int hashCode = nextHashCode();

    private static AtomicInteger nextHashCode = new AtomicInteger();

    /**
     * The difference between successively generated hash codes - turns
     * implicit sequential thread-local IDs into near-optimally spread
     * multiplicative hash values for power-of-two-sized tables.
     */
    private static final int HASH_INCREMENT = 0x61c88647;

    private static int nextHashCode()
    {
    	return nextHashCode.getAndAdd(HASH_INCREMENT);
    }

    public int hashCode()
    {
        return hashCode;
    }

    public Object get(MithraTransaction tx)
    {
        if (tx == null) return null;
        return tx.getTransactionLocalMap().get(this);
    }

    public void set(MithraTransaction tx, Object value)
    {
        tx.getTransactionLocalMap().put(this, value);
    }

    public void remove(MithraTransaction tx)
    {
        tx.getTransactionLocalMap().remove(this);
    }
}
