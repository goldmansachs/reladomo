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

package com.gs.fw.common.mithra.test.util;

import junit.framework.TestCase;

import java.io.*;
import java.sql.Timestamp;

import com.gs.fw.common.mithra.MithraTransactionException;
import com.gs.fw.common.mithra.test.domain.RestrictedEntity;
import com.gs.fw.common.mithra.remote.RemoteTransactionId;

public class ExceptionSerializationTest extends TestCase
{
    public void testMithraExceptionWithoutCause() throws Exception
    {
        MithraTransactionException exception = createBasicException();

        MithraTransactionException after = checkBasicException(exception);
        assertNull(after.getCause());
    }

    public void testMithraExceptionWithGoodCause() throws Exception
    {
        MithraTransactionException exception = createBasicException();
        exception.initCause(new Exception("cause"));
        MithraTransactionException after = checkBasicException(exception);
        assertNotNull(after.getCause());
        assertEquals("cause", after.getCause().getMessage());
    }

    public void testMithraExceptionWithBadCause1() throws Exception
    {
        MithraTransactionException exception = createBasicException();
        exception.initCause(new ExceptionOnSerailizationException("cause123"));
        MithraTransactionException after = checkBasicException(exception);
        assertNotNull(after.getCause());
        assertTrue(after.getCause().getMessage().contains("cause123"));
    }

    public void testMithraExceptionWithBadCause2() throws Exception
    {
        MithraTransactionException exception = createBasicException();
        exception.initCause(new ExceptionOnDeserailizationException("cause123"));
        MithraTransactionException after = checkBasicException(exception);
        assertNotNull(after.getCause());
        assertTrue(after.getCause().getMessage().contains("cause123"));
    }

    public void testSerializationOfDatedObjectsWithSuperclass() throws Exception
    {
        RestrictedEntity entity = new RestrictedEntity(new Timestamp(System.currentTimeMillis()));
        entity.setEntityId(999);
        entity.setEntityName("Name");
        entity.setControlledEntryId(99999);
        RestrictedEntity newEntity = SerializationTestUtil.serializeDeserialize(entity);
        assertFalse(entity.nonPrimaryKeyAttributesChanged(newEntity));

    }

    private MithraTransactionException checkBasicException(MithraTransactionException exception)
            throws IOException, ClassNotFoundException
    {
        MithraTransactionException after = SerializationTestUtil.serializeDeserialize(exception);
        assertEquals("test 1", after.getMessage());
        assertTrue(after.isRetriable());
        assertTrue(after.isTimedOut());
        assertEquals(1, after.getRemoteTransactionId().getServerVmId());
        assertEquals(2, after.getRemoteTransactionId().getTransactionId());
        return after;
    }

    private MithraTransactionException createBasicException()
    {
        MithraTransactionException exception = new MithraTransactionException("test 1");
        exception.setRetriable(true);
        exception.setTimedOut(true);
        exception.setRemoteTransactionId(new RemoteTransactionId(1, 2));
        return exception;
    }

    private static class ExceptionOnSerailizationException extends Exception
    {
        public ExceptionOnSerailizationException(String message)
        {
            super(message);
        }

        private void writeObject(ObjectOutputStream out)
            throws IOException
        {
            throw new IOException("badbadbad");
        }
    }

    private static class ExceptionOnDeserailizationException extends Exception
    {
        public ExceptionOnDeserailizationException(String message)
        {
            super(message);
        }

        private synchronized void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException
        {
            throw new ClassNotFoundException("noclass");
        }
    }
}
