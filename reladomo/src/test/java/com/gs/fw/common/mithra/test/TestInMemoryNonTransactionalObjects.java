
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

import java.sql.Timestamp;

import com.gs.fw.common.mithra.MithraBusinessException;
import com.gs.fw.common.mithra.MithraManager;
import com.gs.fw.common.mithra.MithraTransaction;
import com.gs.fw.common.mithra.TransactionalCommand;
import com.gs.fw.common.mithra.test.domain.*;

public class TestInMemoryNonTransactionalObjects
        extends MithraTestAbstract
{
    private MithraTransaction mithraTransaction;
    private static final Timestamp BUS_DATE_START = new Timestamp(10000);
    private static final Timestamp BUS_DATE_END = new Timestamp(999999);

    private void startTransaction()
    {
        this.mithraTransaction = MithraManager.getInstance().startOrContinueTransaction();
    }

    @Override
    protected void tearDown() throws Exception
    {
        if (this.mithraTransaction != null)
        {
            MithraManager.getInstance().removeTransaction(this.mithraTransaction);
            this.mithraTransaction = null;
        }
        super.tearDown();
    }

    public void testInMemoryNonTransactionalOnCreation()
    {
        this.startTransaction();
        Phone phone = new Phone();
        phone.makeInMemoryNonTransactional();
        phone.setDescription("abc");
        this.mithraTransaction.rollback();
        assertEquals("abc", phone.getDescription());
        phone.makeInMemoryNonTransactional();
        assertEquals("abc", phone.getDescription());
    }

    public void testWithPersistedNonDatedObject()
    {
        try
        {
            Phone phone = new Phone();
            phone.setInventoryId(9999);
            phone.insert();
            phone = PhoneFinder.findOne(PhoneFinder.inventoryId().eq(9999));
            phone.makeInMemoryNonTransactional();
            fail("should not get here");
        }
        catch (MithraBusinessException e)
        {
            assertEquals("Only in memory objects not in transaction can be marked as in memory non transactional", e.getMessage());
        }
    }

    public void testGetNonPersistentCopy()
    {
        Phone phone = new Phone();
        phone.makeInMemoryNonTransactional();
        phone.setDescription("abc");
        assertFalse(phone.isInMemoryState());
        assertTrue(phone.getNonPersistentCopy().isInMemoryState());
    }

    public void testGetDetachedCopy()
    {
        try
        {
            Phone phone = new Phone();
            phone.setDescription("abc");
            phone.makeInMemoryNonTransactional();
            phone.getDetachedCopy();
            fail("Should not get here");
        }
        catch (RuntimeException e)
        {
            assertEquals("Only persisted objects may be detached", e.getMessage());
        }
    }

    public void testIsInMemoryNonTransactional()
    {
        Phone phone = new Phone();
        assertFalse(phone.isInMemoryNonTransactional());
        phone.makeInMemoryNonTransactional();
        assertTrue(phone.isInMemoryNonTransactional());
    }

    public void testInMemoryNonTransactionalAfterTransactionStart()
    {
        this.startTransaction();
        Phone phone = new Phone();
        phone.setDescription("abc");
        phone.makeInMemoryNonTransactional();
        this.mithraTransaction.rollback();
        assertEquals("abc", phone.getDescription());
    }

    public void testNonTransactionalObject()
    {
        Phone phone = new Phone();
        phone.setDescription("abc");
        phone.makeInMemoryNonTransactional();
        assertEquals("abc", phone.getDescription());
    }

    public void testInsideOutsideOfTransaction()
    {
        Phone phone = new Phone();
        phone.setDescription("abc");
        this.startTransaction();
        phone.setInventoryId(10);
        phone.makeInMemoryNonTransactional();
        assertEquals("abc", phone.getDescription());
        assertEquals(10, phone.getInventoryId());
        this.mithraTransaction.rollback();
        assertEquals("abc", phone.getDescription());
        assertEquals(10, phone.getInventoryId());
    }

    public void testExceptionOnInsert()
    {
        try
        {
            Phone phone = new Phone();
            phone.makeInMemoryNonTransactional();
            phone.setInventoryId(100);
            phone.insert();
            fail("Exception should have been thrown");
        }
        catch (RuntimeException e)
        {
            assertEquals("Should never get here", e.getMessage());
        }
    }

    public void testExceptionOnDelete()
    {
        try
        {
            Phone phone = new Phone();
            phone.makeInMemoryNonTransactional();
            phone.setInventoryId(100);
            phone.delete();
            fail("Exception should have been thrown");
        }
        catch (RuntimeException e)
        {
            assertEquals("Cannot delete an object that is not in the database!", e.getMessage());
        }
    }

    public void testWithDatedObject()
    {
        DatedWithMax datedWithMax = new DatedWithMax(new Timestamp(0));
        datedWithMax.setQuantity(123);
        this.startTransaction();
        datedWithMax.setBusinessDateFrom(BUS_DATE_START);
        datedWithMax.setBusinessDateTo(BUS_DATE_END);
        datedWithMax.makeInMemoryNonTransactional();
        this.mithraTransaction.rollback();
        assertEquals(123, datedWithMax.getQuantity(), 0.001);
        assertEquals(BUS_DATE_START, datedWithMax.getBusinessDateFrom());
        assertEquals(BUS_DATE_END, datedWithMax.getBusinessDateTo());
    }

    public void testInMemoryNonTransactionalWithDatedObject()
    {
        DatedWithMax datedWithMax = new DatedWithMax(new Timestamp(0));
        assertFalse(datedWithMax.isInMemoryNonTransactional());
        datedWithMax.makeInMemoryNonTransactional();
        assertTrue(datedWithMax.isInMemoryNonTransactional());
    }

    public void testWithDatedPersistedObject()
    {
        MithraManager.getInstance().executeTransactionalCommand(new TransactionalCommand<Object>()
        {
            public Object executeTransaction(MithraTransaction tx) throws Throwable
            {
                DatedWithDescendingSequence descendingSequence = new DatedWithDescendingSequence(BUS_DATE_START);
                descendingSequence.setBalanceId(12);
                descendingSequence.setAcmapCode("A");
                descendingSequence.insert();
                return null;
            }
        });
        DatedWithDescendingSequence descendingSequence = DatedWithDescendingSequenceFinder.findOne(DatedWithDescendingSequenceFinder.acmapCode().eq("A").
                and(DatedWithDescendingSequenceFinder.businessDate().eq(BUS_DATE_START)));
        try
        {
            descendingSequence.makeInMemoryNonTransactional();
        }
        catch (MithraBusinessException e)
        {
            assertEquals("Only in memory objects not in transaction can be marked as in memory non transactional", e.getMessage());
        }
    }

    public void testExceptionOnDatedObjectInsert()
    {
        try
        {
            DatedWithDescendingSequence descendingSequence = new DatedWithDescendingSequence(new Timestamp(0));
            descendingSequence.setBalanceId(12);
            descendingSequence.makeInMemoryNonTransactional();
            descendingSequence.insert();

            fail("Exception should have been thrown");
        }
        catch (RuntimeException e)
        {
            assertEquals("In memory non-transactional objects cannot be inserted!", e.getMessage());
        }
    }
}
