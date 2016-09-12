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

package com.gs.fw.common.mithra.test;

import com.gs.fw.common.mithra.MithraManagerProvider;
import com.gs.fw.common.mithra.TransactionalCommand;
import com.gs.fw.common.mithra.MithraTransaction;
import com.gs.fw.common.mithra.test.domain.*;

import java.sql.Timestamp;

import junit.framework.TestCase;


public class TestGeneratedCompoundPrimaryKeys extends MithraTestAbstract
{

    public Class[] getRestrictedClassList()
    {
        return new Class[]
        {
            SpecialAccount.class,
            MithraTestSequence.class
        };
    }

    public void testInsertingObjectsWithCompoundPrimaryKeysUsingDifferentPKGeneratorStrategies()
    {
         MithraManagerProvider.getMithraManager().executeTransactionalCommand(
                new TransactionalCommand()
                {
                    public Object executeTransaction(MithraTransaction tx) throws Throwable
                    {
                        SpecialAccount specialAccount0 = new SpecialAccount();
                        specialAccount0.setDeskId("A");
                        specialAccount0.setSpecialAccountDescription("Account Description 0");
                        specialAccount0.setSpecialAccountOpeningDate(new Timestamp(System.currentTimeMillis()));
                        long accountId0 = specialAccount0.generateAndSetSpecialAccountId();
                        assertEquals(100000000, accountId0);
                        long code0 = specialAccount0.generateAndSetAccountSpecialCode();
                        assertEquals(11, code0);


                        SpecialAccount specialAccount1 = new SpecialAccount();
                        specialAccount1.setDeskId("A");
                        specialAccount1.setSpecialAccountDescription("Account Description 0");
                        specialAccount1.setSpecialAccountOpeningDate(new Timestamp(System.currentTimeMillis()));
                        long accountId1 = specialAccount1.generateAndSetSpecialAccountId();
                        assertEquals(100000001, accountId1);
                        assertEquals(0, specialAccount1.getAccountSpecialCode());


                        SpecialAccount specialAccount2 = new SpecialAccount();
                        specialAccount2.setDeskId("A");
                        specialAccount2.setSpecialAccountDescription("Account Description 0");
                        specialAccount2.setSpecialAccountOpeningDate(new Timestamp(System.currentTimeMillis()));
                        long code2 = specialAccount2.generateAndSetAccountSpecialCode();
                        assertEquals(0, specialAccount2.getSpecialAccountId());
                        assertEquals(12, code2);

                        specialAccount0.insert();
                        specialAccount1.insert();
                        specialAccount2.insert();
                        assertEquals(13,specialAccount1.getAccountSpecialCode());
                        assertEquals(100000002, specialAccount2.getSpecialAccountId());

                        return null;
                    }
                });
    }

    public void testInsertingListWithCompoundPrimaryKeysUsingDifferentPKGeneratorStrategies()
    {
        MithraManagerProvider.getMithraManager().executeTransactionalCommand(
                new TransactionalCommand()
                {
                    public Object executeTransaction(MithraTransaction tx) throws Throwable
                    {
                        SpecialAccountList list = new SpecialAccountList();
                        SpecialAccount specialAccount0 = null;

                        for(int i = 0; i < 20; i++)
                        {
                            specialAccount0 = new SpecialAccount();
                            specialAccount0.setDeskId("A");
                            specialAccount0.setSpecialAccountDescription("Account Description 0");
                            specialAccount0.setSpecialAccountOpeningDate(new Timestamp(System.currentTimeMillis()));
                            list.add(specialAccount0);
                        }
                        list.insertAll();
                        for(int i = 0; i < list.size(); i++)
                        {
                            specialAccount0 = (SpecialAccount)list.get(i);
                            assertTrue((specialAccount0.getSpecialAccountId() >= 100000000 && specialAccount0.getSpecialAccountId() < 100000020) && (specialAccount0.getAccountSpecialCode() > 10 && specialAccount0.getAccountSpecialCode() < 31 ));
                        }
                        return null;
                    }
                });
    }

}
