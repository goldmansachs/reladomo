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

package com.gs.fw.common.mithra.finder;

import com.gs.fw.common.mithra.MithraManagerProvider;
import com.gs.fw.common.mithra.MithraTransaction;
import com.gs.fw.common.mithra.transaction.TransactionLocal;



public class UpdateCountHolderImpl implements UpdateCountHolder
{
    private volatile int nonTxUpdateCount = 100000000;
    private TransactionLocal txUpdateCount = new TransactionLocal();

    public void setUpdateCountDetachedMode(boolean isDetachedMode)
    {
        MithraTransaction tx = MithraManagerProvider.getMithraManager().zGetCurrentTransactionWithNoCheck();
        if (tx != null)
        {
            MutableIntegerCounters perThread = this.getOrCreateCounters(tx);
            perThread.setDetachedMode(isDetachedMode);
        }
    }

    public int getUpdateCount()
    {
        MithraTransaction tx = MithraManagerProvider.getMithraManager().zGetCurrentTransactionWithNoCheck();
        if (tx != null)
        {
            MutableIntegerCounters perThread = (MutableIntegerCounters) txUpdateCount.get(tx);
            if (perThread == null) return 0;
            return perThread.getValue();
        }
        return this.nonTxUpdateCount;
    }

    public int getNonTxUpdateCount()
    {
        return this.nonTxUpdateCount;
    }

    public void incrementUpdateCount()
    {
        MithraTransaction tx = MithraManagerProvider.getMithraManager().zGetCurrentTransactionWithNoCheck();
        if (tx == null)
        {
            this.nonTxUpdateCount++;
        }
        else
        {
            MutableIntegerCounters perThread = this.getOrCreateCounters(tx);
            perThread.incrementValue();
        }
    }

    public void commitUpdateCount()
    {
        MithraTransaction tx = MithraManagerProvider.getMithraManager().zGetCurrentTransactionWithNoCheck();
        MutableIntegerCounters perThread = (MutableIntegerCounters) txUpdateCount.get(tx);
        if (perThread != null && perThread.getValue() > 0)
        {
            this.nonTxUpdateCount++;
            perThread.resetValue();
        }
    }

    public void rollbackUpdateCount()
    {
        MithraTransaction tx = MithraManagerProvider.getMithraManager().zGetCurrentTransactionWithNoCheck();
        MutableIntegerCounters perThread = (MutableIntegerCounters) txUpdateCount.get(tx);
        if (perThread != null)
        {
            perThread.resetValue();
        }
    }

    private MutableIntegerCounters getOrCreateCounters(MithraTransaction tx)
    {
        MutableIntegerCounters perThread = (MutableIntegerCounters) txUpdateCount.get(tx);
        if (perThread == null)
        {
            perThread = new MutableIntegerCounters();
            txUpdateCount.set(tx, perThread);
        }
        return perThread;
    }

    private static class MutableIntegerCounters
    {
        private int detachedModeCount = 0;
        private int mainValue = 0;
        private int detachedValue = 0;

        public void setDetachedMode(boolean newDetachedMode)
        {
            if (newDetachedMode)
            {
                this.detachedModeCount++;
            }
            else
            {
                this.detachedModeCount--;
                if (this.detachedModeCount == 0)
                {
                    if (this.detachedValue > 0)
                    {
                        this.mainValue++;
                    }
                    this.detachedValue = 0;
                }
            }
        }

        public int getValue()
        {
            return this.mainValue;
        }

        public void incrementValue()
        {
            if (this.detachedModeCount > 0)
            {
                this.detachedValue++;
            }
            else
            {
                this.mainValue++;
            }
        }

        public void resetValue()
        {
            this.detachedValue = 0;
            this.mainValue = 0;
        }
    }
}
