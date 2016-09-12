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

package com.gs.fw.common.mithra.test.domain.evo;

import com.gs.fw.common.mithra.MithraDataObject;

public class EvoType1TxnTypesDatabaseObject extends EvoType1TxnTypesDatabaseObjectAbstract
{

    protected EvoType1TxnTypes constructEvoType1TxnTypes(MithraDataObject data)
    {
        EvoType1TxnTypesData typesData = (EvoType1TxnTypesData) data;
        if (typesData.getPkCharAttribute() == EvoType1TxnTypes.TYPE_A)
        {
            return new EvoType1TxnTypesA();
        }
        else if (typesData.getPkCharAttribute() == EvoType1TxnTypes.TYPE_B)
        {
            return new EvoType1TxnTypesB();
        }
        else
        {
            return null;
        }
    }
}
