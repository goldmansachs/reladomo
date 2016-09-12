
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

import com.gs.fw.common.mithra.MithraTransaction;

import java.sql.Timestamp;

public interface TinyBalanceInterface
{

    public void setAcmapCode(String a);
    public String getAcmapCode();
    public void setBalanceId(int nb);
    public int getBalanceId();
    public void insert();
    public void setQuantity(double d);
    public double getQuantity();

    boolean zIsParticipatingInTransaction(MithraTransaction tx);
    public Timestamp getBusinessDateFrom();
    public Timestamp getProcessingDateFrom();
    public Timestamp getBusinessDateTo();
    public Timestamp getProcessingDateTo();

    public void setBusinessDateFrom(Timestamp ts);
    public void setProcessingDateFrom(Timestamp ts);
    public void setBusinessDateTo(Timestamp ts);
    public void setProcessingDateTo(Timestamp ts);

    public TinyBalanceInterface getNonPersistentCopy();

    public void setQuantityNull();

    public void copyNonPrimaryKeyAttributesFrom(TinyBalanceInterface inface);
    public void copyNonPrimaryKeyAttributesUntilFrom(TinyBalanceInterface inface, Timestamp exclusiveUntil);
    public void insertWithIncrement();
    public void incrementQuantity(double dble);
    public void incrementQuantityUntil(double dble, Timestamp ts);
    public void setQuantityUntil(double dble, Timestamp ts);

    public void insertUntil(Timestamp ts);
    public void insertWithIncrementUntil(Timestamp ts);

    public void terminate();
    public void purge();

    public void insertForRecovery();

    public void terminateUntil(Timestamp ts);

    public void inactivateForArchiving(Timestamp ts1, Timestamp ts2);

}
