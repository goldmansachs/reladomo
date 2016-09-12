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

package com.gs.fw.common.mithra.notification.listener;

import com.gs.fw.common.mithra.MithraObjectPortal;
import com.gs.fw.common.mithra.MithraDatedTransactionalObject;
import com.gs.fw.common.mithra.MithraTransactionalObject;
import com.gs.fw.common.mithra.behavior.state.PersistedState;
import com.gs.fw.common.mithra.finder.Operation;

import java.util.List;


public abstract class AbstractNonDatedMithraNotificationListener extends AbstractMithraNotificationListener
{

    public AbstractNonDatedMithraNotificationListener(MithraObjectPortal mithraObjectPortal)
    {
        super(mithraObjectPortal);
    }

    public void onMassDelete(final Operation op)
    {
        List result = getMithraObjectPortal().findForMassDeleteInMemory(op, null);
        if (result != null)
        {
            getMithraObjectPortal().getCache().removeAll(result);
//            for(int i = 0; i < result.size(); i++)
//            {
//                MithraTransactionalObject txObject = (MithraTransactionalObject)result.get(i);
//                txObject.zSetNonTxPersistenceState(PersistedState.DELETED);
//            }
        }
        getMithraObjectPortal().incrementClassUpdateCount();
    }
}
