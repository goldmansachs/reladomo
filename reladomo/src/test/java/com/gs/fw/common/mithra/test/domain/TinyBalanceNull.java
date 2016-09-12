
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

package com.gs.fw.common.mithra.test.domain;

import java.sql.Timestamp;

public class TinyBalanceNull extends TinyBalanceNullAbstract implements TinyBalanceInterface
{

    public TinyBalanceNull(Timestamp businessDate, Timestamp processingDate)
    {
        super(businessDate, processingDate);
    }

    public TinyBalanceNull(Timestamp businessDate)
    {
        super(businessDate);
    }

    public void copyNonPrimaryKeyAttributesFrom(TinyBalanceInterface inface)
    {
        this.copyNonPrimaryKeyAttributesFrom((TinyBalanceNullAbstract)inface);
    }

    public void copyNonPrimaryKeyAttributesUntilFrom(TinyBalanceInterface inface, Timestamp exclusiveUntil)
    {
        this.copyNonPrimaryKeyAttributesUntilFrom((TinyBalanceNullAbstract)inface, exclusiveUntil);
    }
}
