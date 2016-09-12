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

package com.gs.fw.common.mithra.behavior;

import java.sql.Timestamp;

import com.gs.fw.common.mithra.MithraDatedTransactionalObject;
import com.gs.fw.common.mithra.attribute.update.AttributeUpdateWrapper;
import com.gs.fw.common.mithra.attribute.update.DoubleIncrementUpdateWrapper;
import com.gs.fw.common.mithra.attribute.update.BigDecimalIncrementUpdateWrapper;



public interface TemporalDirector
{

    public void insert(MithraDatedTransactionalObject mithraObject, TemporalContainer container);

    public void insertWithIncrement(MithraDatedTransactionalObject mithraObject, TemporalContainer container);

    public void update(MithraDatedTransactionalObject obj, TemporalContainer container, AttributeUpdateWrapper updateWrapper);

    public void increment(MithraDatedTransactionalObject obj, TemporalContainer container, AttributeUpdateWrapper updateWrapper);

    public void terminate(MithraDatedTransactionalObject obj, TemporalContainer container);

    public void purge(MithraDatedTransactionalObject obj, TemporalContainer container);

    public void insertForRecovery(MithraDatedTransactionalObject obj, TemporalContainer container);

    public void incrementUntil(MithraDatedTransactionalObject obj, TemporalContainer container, AttributeUpdateWrapper updateWrapper, Timestamp until);

    public void updateUntil(MithraDatedTransactionalObject obj, TemporalContainer container, AttributeUpdateWrapper updateWrapper, Timestamp until);

    public void inPlaceUpdate(MithraDatedTransactionalObject obj, TemporalContainer container, AttributeUpdateWrapper updateWrapper);

    public void insertWithIncrementUntil(MithraDatedTransactionalObject obj, TemporalContainer container, Timestamp exclusiveUntil);

    public void insertUntil(MithraDatedTransactionalObject obj, TemporalContainer container, Timestamp exclusiveUntil);

    public void inactivateForArchiving(MithraDatedTransactionalObject obj, TemporalContainer container, Timestamp processingDateTo, Timestamp businessDateTo);

    public void terminateUntil(MithraDatedTransactionalObject obj, TemporalContainer container, Timestamp until);
}
