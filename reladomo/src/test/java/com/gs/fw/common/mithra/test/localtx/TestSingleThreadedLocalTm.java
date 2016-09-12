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

package com.gs.fw.common.mithra.test.localtx;

import com.gs.fw.common.mithra.transaction.LocalTm;
import junit.framework.TestCase;

import javax.transaction.*;
import javax.transaction.xa.XAResource;
import javax.transaction.xa.Xid;
import java.io.*;


public class TestSingleThreadedLocalTm extends TestCase
{

    private TransactionManager tm;

    public TransactionManager getTm()
    {
        return tm;
    }

    protected void setUp() throws Exception
    {
        this.tm = new LocalTm();
    }

    private void postTransactionVerify()
            throws SystemException
    {
        assertEquals(Status.STATUS_NO_TRANSACTION, tm.getStatus());
        assertNull(tm.getTransaction());
    }

    public byte[] serialize(Object o) throws IOException
    {
        byte[] pileOfBytes = null;
        ByteArrayOutputStream bos = new ByteArrayOutputStream(2000);
        ObjectOutputStream oos = new ObjectOutputStream(bos);
        oos.writeObject(o);
        oos.flush();
        bos.flush();
        pileOfBytes = bos.toByteArray();
        bos.close();
        return pileOfBytes;
    }

    public Object deserialize(byte[] input) throws IOException, ClassNotFoundException
    {
        ByteArrayInputStream bis  = new ByteArrayInputStream(input);
        ObjectInputStream ois = new ObjectInputStream(bis);
        Object result = ois.readObject();
        ois.close();
        bis.close();
        return result;
    }

    public void testCommitNoResource() throws Exception
    {
        this.tm.begin();
        Transaction tx = this.tm.getTransaction();
        tx.commit();
        postTransactionVerify();
    }

    public void testCommitOneResource() throws Exception
    {
        ExecutionOrder.start();
        this.tm.begin();
        Transaction tx = this.tm.getTransaction();
        ExceptionThrowingXaResource res = new ExceptionThrowingXaResource(false);
        assertTrue(tx.enlistResource(res));
        assertTrue(tx.delistResource(res, XAResource.TMSUCCESS));
        tx.commit();
        ExecutionOrder.verifyForThread(res, "start");
        ExecutionOrder.verifyForThread(res, "end");
        ExecutionOrder.verifyForThread(res, "commit");
        res.verify();
        res.verifyStartEndCommit(XAResource.TMNOFLAGS, XAResource.TMSUCCESS, true);
        postTransactionVerify();
    }

    public void testRollbackNoResource() throws Exception
    {
        this.tm.begin();
        Transaction tx = this.tm.getTransaction();
        tx.rollback();
        postTransactionVerify();
    }

    public void testRollbackOneResource() throws Exception
    {
        ExecutionOrder.start();
        this.tm.begin();
        Transaction tx = this.tm.getTransaction();
        ExceptionThrowingXaResource res = new ExceptionThrowingXaResource(false);
        res.setExpectedCommit(0);
        res.setExpectedRollack(1);
        assertTrue(tx.enlistResource(res));
        assertTrue(tx.delistResource(res, XAResource.TMFAIL));
        tx.rollback();
        ExecutionOrder.verifyForThread(res, "start");
        ExecutionOrder.verifyForThread(res, "end");
        ExecutionOrder.verifyForThread(res, "rollback");
        res.verify();
        res.verifyStartEndCommit(XAResource.TMNOFLAGS, XAResource.TMFAIL, false);
        postTransactionVerify();
    }

    public void testCommitTwoResources() throws Exception
    {
        ExecutionOrder.start();
        this.tm.begin();
        Transaction tx = this.tm.getTransaction();
        ExceptionThrowingXaResource res = new ExceptionThrowingXaResource(false);
        ExceptionThrowingXaResource res2 = new ExceptionThrowingXaResource(false);
        assertTrue(tx.enlistResource(res));
        assertTrue(tx.enlistResource(res2));
        assertTrue(tx.delistResource(res, XAResource.TMSUCCESS));
        assertTrue(tx.delistResource(res2, XAResource.TMSUCCESS));
        tx.commit();
        ExecutionOrder.verifyForThread(res, "start");
        ExecutionOrder.verifyForThread(res2, "isSameRM");
        ExecutionOrder.verifyForThread(res2, "start");
        ExecutionOrder.verifyForThread(res, "end");
        ExecutionOrder.verifyForThread(res2, "end");
        ExecutionOrder.verifyForThread(res2, "prepare");
        ExecutionOrder.verifyForThread(res, "commit");
        ExecutionOrder.verifyForThread(res2, "commit");
        res.verify();
        res.verifyStartEndCommit(XAResource.TMNOFLAGS, XAResource.TMSUCCESS, true);
        res2.verify();
        res2.verifyStartEndCommit(XAResource.TMNOFLAGS, XAResource.TMSUCCESS, false);
        postTransactionVerify();

    }

    public void testCommitTwoResourcesNoDelist() throws Exception
    {
        ExecutionOrder.start();
        this.tm.begin();
        Transaction tx = this.tm.getTransaction();
        ExceptionThrowingXaResource res = new ExceptionThrowingXaResource(false);
        ExceptionThrowingXaResource res2 = new ExceptionThrowingXaResource(false);
        assertTrue(tx.enlistResource(res));
        assertTrue(tx.enlistResource(res2));
        tx.commit();
        ExecutionOrder.verifyForThread(res, "start");
        ExecutionOrder.verifyForThread(res2, "isSameRM");
        ExecutionOrder.verifyForThread(res2, "start");
        ExecutionOrder.verifyForThread(res, "end");
        ExecutionOrder.verifyForThread(res2, "end");
        ExecutionOrder.verifyForThread(res2, "prepare");
        ExecutionOrder.verifyForThread(res, "commit");
        ExecutionOrder.verifyForThread(res2, "commit");
        res.verify();
        res.verifyStartEndCommit(XAResource.TMNOFLAGS, XAResource.TMSUCCESS, true);
        res2.verify();
        res2.verifyStartEndCommit(XAResource.TMNOFLAGS, XAResource.TMSUCCESS, false);
        postTransactionVerify();

    }

    public void testCommitTwoResourcesSameRm() throws Exception
    {
        ExecutionOrder.start();
        this.tm.begin();
        Transaction tx = this.tm.getTransaction();
        ExceptionThrowingXaResource res = new ExceptionThrowingXaResource(false);
        ExceptionThrowingXaResource res2 = new ExceptionThrowingXaResource(false);
        res2.setExpectedCommit(0);
        res.setSameRmResource(res2);
        res2.setSameRmResource(res);
        assertTrue(tx.enlistResource(res));
        assertTrue(tx.enlistResource(res2));
        tx.commit();
        ExecutionOrder.verifyForThread(res, "start");
        ExecutionOrder.verifyForThread(res2, "isSameRM");
        ExecutionOrder.verifyForThread(res2, "start");
        ExecutionOrder.verifyForThread(res, "end");
        ExecutionOrder.verifyForThread(res2, "end");
        ExecutionOrder.verifyForThread(res, "commit");
        res.verify();
        res.verifyStartEndCommit(XAResource.TMNOFLAGS, XAResource.TMSUCCESS, true);
        res2.verify();
        res2.verifyStartEndCommit(XAResource.TMJOIN, XAResource.TMSUCCESS, false);
        postTransactionVerify();

    }

    public void testSerializable() throws Exception
    {
        ExecutionOrder.start();
        this.tm.begin();
        Transaction tx = this.tm.getTransaction();
        ExceptionThrowingXaResource res = new ExceptionThrowingXaResource(false);
        assertTrue(tx.enlistResource(res));
        assertTrue(tx.delistResource(res, XAResource.TMSUCCESS));
        Xid reconstituted = (Xid) this.deserialize(this.serialize(res.getXid()));
        assertEquals(res.getXid(), reconstituted);
        tx.commit();
        res.verify();
        res.verifyStartEndCommit(XAResource.TMNOFLAGS, XAResource.TMSUCCESS, true);
        postTransactionVerify();
    }

    public void testRollbackOnlyOneResource() throws Exception
    {
        ExecutionOrder.start();
        this.tm.begin();
        Transaction tx = this.tm.getTransaction();
        ExceptionThrowingXaResource res = new ExceptionThrowingXaResource(false);
        res.setExpectedCommit(0);
        res.setExpectedRollack(1);
        assertTrue(tx.enlistResource(res));
        assertTrue(tx.delistResource(res, XAResource.TMSUCCESS));
        tx.setRollbackOnly();
        try
        {
            tx.commit();
            fail("tx must not commit");
        }
        catch (RollbackException e)
        {
            // expected
        }
        ExecutionOrder.verifyForThread(res, "start");
        ExecutionOrder.verifyForThread(res, "end");
        ExecutionOrder.verifyForThread(res, "rollback");
        res.verify();
        res.verifyStartEndCommit(XAResource.TMNOFLAGS, XAResource.TMSUCCESS, false);
        postTransactionVerify();
    }

    public void testRollbackOnlyTwoResource() throws Exception
    {
        ExecutionOrder.start();
        this.tm.begin();
        Transaction tx = this.tm.getTransaction();
        ExceptionThrowingXaResource res = new ExceptionThrowingXaResource(false);
        ExceptionThrowingXaResource res2 = new ExceptionThrowingXaResource(false);
        res.setExpectedCommit(0);
        res.setExpectedRollack(1);
        res2.setExpectedCommit(0);
        res2.setExpectedRollack(1);
        assertTrue(tx.enlistResource(res));
        assertTrue(tx.enlistResource(res2));
        assertTrue(tx.delistResource(res, XAResource.TMSUCCESS));
        assertTrue(tx.delistResource(res2, XAResource.TMSUCCESS));
        tx.setRollbackOnly();
        try
        {
            tx.commit();
            fail("tx must not commit");
        }
        catch (RollbackException e)
        {
            // expected
        }
        ExecutionOrder.verifyForThread(res, "start");
        ExecutionOrder.verifyForThread(res2, "isSameRM");
        ExecutionOrder.verifyForThread(res2, "start");
        ExecutionOrder.verifyForThread(res, "end");
        ExecutionOrder.verifyForThread(res2, "end");
        ExecutionOrder.verifyForThread(res, "rollback");
        ExecutionOrder.verifyForThread(res2, "rollback");
        res.verify();
        res.verifyStartEndCommit(XAResource.TMNOFLAGS, XAResource.TMSUCCESS, false);
        res2.verify();
        res2.verifyStartEndCommit(XAResource.TMNOFLAGS, XAResource.TMSUCCESS, false);
        postTransactionVerify();

    }

}
