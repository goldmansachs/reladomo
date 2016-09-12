
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

import com.gs.fw.common.mithra.MithraBusinessException;
import com.gs.fw.common.mithra.MithraDatedObject;
import com.gs.fw.common.mithra.MithraTransactionalObject;
import com.gs.fw.common.mithra.test.domain.ParaDesk;
import com.gs.fw.common.mithra.test.domain.ParaDeskFinder;
import com.gs.fw.common.mithra.test.domain.TamsAccount;
import com.gs.fw.common.mithra.test.domain.TamsAccountFinder;
import junit.framework.Assert;

import java.sql.Timestamp;
import java.util.Date;

public class TestSetReadOnlyAttributes
extends MithraTestAbstract
{

    private static final Class[] RESTRICTED_CLASS_LIST =
            {
                ParaDesk.class,
                TamsAccount.class
            };

    public Class[] getRestrictedClassList()
    {
        return TestSetReadOnlyAttributes.RESTRICTED_CLASS_LIST;
    }

    public void testSetAttributeReadOnlyPersisted()
    {
        final ParaDesk existingDesk = ParaDeskFinder.findOne(ParaDeskFinder.deskIdString().eq("rnd"));
        Assert.assertNotNull(existingDesk);
        Assert.assertFalse(existingDesk instanceof MithraTransactionalObject);  // i.e. "read only"
        Assert.assertFalse(existingDesk instanceof MithraDatedObject);  // i.e. "not dated"

        try
        {
            existingDesk.setDeskIdString("Changed!");
            Assert.fail("Should have thrown an exception");
        }
        catch (MithraBusinessException e)
        {
            Assert.assertEquals("rnd", existingDesk.getDeskIdString());
        }

        final Date closedDate = existingDesk.getClosedDate();
        try
        {
            existingDesk.setClosedDate(new Date());
            Assert.fail("Should have thrown an exception");
        }
        catch (MithraBusinessException e)
        {
            Assert.assertEquals(closedDate, existingDesk.getClosedDate());
        }
    }

    public void testSetAttributeReadOnlyNotPersisted()  // todo:  is this test worth it?
    {
        final ParaDesk newDesk = new ParaDesk();
        Assert.assertFalse(newDesk instanceof MithraTransactionalObject);  // i.e. "read only"
        Assert.assertFalse(newDesk instanceof MithraDatedObject);  // i.e. "not dated"
        newDesk.setDeskIdString("tis");
    }


    public void testSetAttributeDatedReadOnlyPersisted()
    {
        final Timestamp asOf = new Timestamp(System.currentTimeMillis());
        final TamsAccount existingAccount = TamsAccountFinder.findOne(TamsAccountFinder.accountNumber().eq("7410161001").and(
                                                     TamsAccountFinder.deskId().eq("A")).and(TamsAccountFinder.businessDate().eq(asOf)));
        Assert.assertNotNull(existingAccount);
        Assert.assertFalse(existingAccount instanceof MithraTransactionalObject);  // i.e. "read only"
        Assert.assertTrue(existingAccount instanceof MithraDatedObject);  // i.e. "dated"

        try
        {
            existingAccount.setAccountNumber("Changed!");
            Assert.fail("Should have thrown an exception");
        }
        catch (MithraBusinessException e)
        {
            Assert.assertEquals("7410161001", existingAccount.getAccountNumber());
        }

        final Timestamp businessDateFrom = existingAccount.getBusinessDateFrom();
        try
        {
            existingAccount.setBusinessDateFrom(new Timestamp(0));
            Assert.fail("Should have thrown an exception");
        }
        catch (MithraBusinessException e)
        {
            Assert.assertEquals(businessDateFrom, existingAccount.getBusinessDateFrom());
        }
    }

    public void testSetAttributeDatedReadOnlyNotPersisted()  // todo:  is this test worth it?
    {
        final Timestamp asOf = new Timestamp(System.currentTimeMillis());
        final TamsAccount newAccount = new TamsAccount(asOf, asOf);
        Assert.assertFalse(newAccount instanceof MithraTransactionalObject);  // i.e. "read only"
        Assert.assertTrue(newAccount instanceof MithraDatedObject);  // i.e. "dated"
        newAccount.setDeskId("tis");
    }

}
