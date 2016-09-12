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

package com.gs.fw.common.mithra.behavior.txparticipation;


public class ReadCacheWithOptimisticLockingTxParticipationMode extends TxParticipationMode
{

    private static final ReadCacheWithOptimisticLockingTxParticipationMode instance = new ReadCacheWithOptimisticLockingTxParticipationMode();

    public ReadCacheWithOptimisticLockingTxParticipationMode()
    {
    }

    public static ReadCacheWithOptimisticLockingTxParticipationMode getInstance()
    {
        return ReadCacheWithOptimisticLockingTxParticipationMode.instance;
    }

    public boolean mustLockOnRead()
    {
        return false;
    }

    public boolean mustParticipateInTxOnRead()
    {
        return false;
    }

    public boolean isOptimisticLocking()
    {
        return true;
    }

    public Object readResolve()
    {
        return ReadCacheWithOptimisticLockingTxParticipationMode.getInstance();
    }
}
