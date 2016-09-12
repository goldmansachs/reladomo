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

package com.gs.fw.common.mithra.behavior.state;

import com.gs.fw.common.mithra.DatedTransactionalState;
import com.gs.fw.common.mithra.MithraDatedTransactionalObject;
import com.gs.fw.common.mithra.MithraTransactionalObject;
import com.gs.fw.common.mithra.TransactionalState;
import com.gs.fw.common.mithra.behavior.DatedTransactionalBehavior;
import com.gs.fw.common.mithra.behavior.TransactionalBehavior;


public class NoTransactionBehaviorChooser implements TransactionalBehaviorChooser
{

    public DatedTransactionalBehavior getDatedTransactionalBehavior(MithraDatedTransactionalObject mto,
            DatedTransactionalState transactionalState, int persistenceState)
    {
        return DatedPersistenceState.getTransactionalBehaviorForNoTransaction(mto, transactionalState, persistenceState);
    }

    public TransactionalBehavior getTransactionalBehaviorForWriteWithWaitIfNecessary(MithraTransactionalObject mithraTransactionalObject, TransactionalState transactionalState, int persistenceState)
    {
        return PersistenceState.getTransactionalBehaviorForNoTransactionWriteWithWaitIfNecessary(transactionalState, persistenceState, mithraTransactionalObject);
    }

    public TransactionalBehavior getTransactionalBehaviorForReadWithWaitIfNecessary(MithraTransactionalObject mithraTransactionalObject, TransactionalState transactionalState, int persistenceState)
    {
        return PersistenceState.getTransactionalBehaviorForNoTransactionWithWaitIfNecessary(transactionalState, persistenceState, mithraTransactionalObject);
    }
}
