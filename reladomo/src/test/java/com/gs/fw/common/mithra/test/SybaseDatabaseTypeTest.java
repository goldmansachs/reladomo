
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

import com.gs.fw.common.mithra.databasetype.SybaseDatabaseType;
import junit.framework.TestCase;

import java.io.IOException;
import java.sql.SQLException;

public class SybaseDatabaseTypeTest
        extends TestCase
{
    public void testIsKilledConnectionException()
    {
        SybaseDatabaseType sybaseDatabaseType = SybaseDatabaseType.getInstance();
        assertFalse(sybaseDatabaseType.isKilledConnectionException(new IOException()));
        assertTrue(sybaseDatabaseType.isKilledConnectionException(new SQLException("com.sybase.jdbc4.jdbc.SybConnectionDeadException")));
    }

    public void testIsConnectionDeadWithoutRecursion()
    {
        SybaseDatabaseType sybaseDatabaseType = SybaseDatabaseType.getInstance();
        SQLException exception = new SQLException("com.sybase.jdbc4.jdbc.SybConnectionDeadException");
        assertFalse(sybaseDatabaseType.isConnectionDeadWithoutRecursion(exception));
        exception= new SQLException("ASE has run out of LOCKS. Re-run your command when there are fewer active users, or contact a user with System Administrator (SA) role to reconfigure ASE with more LOCKS.",
                "ZZZZZ", 1204);
        assertTrue(sybaseDatabaseType.isConnectionDeadWithoutRecursion(exception));
    }
}
