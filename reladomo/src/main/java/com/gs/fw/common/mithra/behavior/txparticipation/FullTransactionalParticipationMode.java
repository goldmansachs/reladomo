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


public class FullTransactionalParticipationMode extends TxParticipationMode
{

    private static final FullTransactionalParticipationMode instance = new FullTransactionalParticipationMode();

    public static FullTransactionalParticipationMode getInstance()
    {
        return instance;
    }

    public FullTransactionalParticipationMode()
    {
    }

    public boolean mustLockOnRead()
    {
        return true;
    }

    public boolean mustParticipateInTxOnRead()
    {
        return true;
    }

    public Object readResolve()
    {
        return getInstance();
    }
}
