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

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;


public abstract class TxParticipationMode implements Externalizable
{

    public abstract boolean mustLockOnRead();

    public abstract boolean mustParticipateInTxOnRead();

    protected TxParticipationMode()
    {
        // for exernalizable
    }

    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException
    {
        // nothing to do. Pure behavior enum class
    }

    public void writeExternal(ObjectOutput out) throws IOException
    {
        // nothing to do
    }

    public abstract Object readResolve();

    public boolean isOptimisticLocking()
    {
        return false;
    }
}
