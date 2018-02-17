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
// Portions copyright Hiroshi Ito. Licensed under Apache 2.0 license

package com.gs.fw.common.mithra.test;

import com.gs.fw.common.mithra.test.domain.bcp.TrailingSpaces;
import com.gs.fw.common.mithra.test.domain.bcp.TrailingSpacesFinder;
import com.gs.fw.common.mithra.test.domain.bcp.TrailingSpacesList;
import junit.framework.Assert;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

public class SybaseTrailingSpacesTest extends SybaseBcpTestAbstract
{
    private static final Logger LOG = LoggerFactory.getLogger(SybaseTrailingSpacesTest.class.getName());

    private static final String TEST_STRING = "Circumspect    ";
    private static final String TRIMMED_STRING = "Circumspect";
    private static final String PADDED_STRING = "Circumspect                   ";

    public void testWithTrailingSpaces()
    {
        int count = 2000;
        String dropSql = "DROP TABLE TRAILING_SPACES";
        String createSql = "create table TRAILING_SPACES "
                + "(ID int not null,"
                + " VARCHAR_NOTNULL varchar(30) not null,"
                + " CHAR_NOTNULL char(30) not null,"
                + " VARCHAR_NULLABLE varchar(30) null,"
                + " CHAR_NULLABLE char(30) null,"
                + " DESCRIPTION varchar(24) null"
                + ")";
        // Re-create table using specific custom DDL
        SybaseTestConnectionManager connectionManager = SybaseTestConnectionManager.getInstance();
        Connection connection = null;
        Statement statement = null;
        try {
            connection = connectionManager.getConnection();
            connection.setAutoCommit(true);
            statement = connection.createStatement();
            statement.execute(dropSql);
            statement.execute(createSql);
        }
        catch (SQLException e) {
            LOG.error("Problem using SQL connection", e);
            fail();
        }
        finally {
            if (statement != null) {
                try {
                    statement.close();
                }
                catch (SQLException e) {
                    LOG.error("Exception closing statement", e);
                }
            }
            if (connection != null) {
                try {
                    connection.close();
                }
                catch (SQLException e) {
                    LOG.error("Exception closing connection", e);
                }
            }
        }

        // Insert a row using non-bulk insert
        new TrailingSpaces(0, "Plain Insert", TEST_STRING).insert();

        // Bulk insert rows
        TrailingSpacesList bulkItems = new TrailingSpacesList(count);
        for (int i = 1; i < count; i++) {
            bulkItems.add(new TrailingSpaces(i, "Bulk Insert", TEST_STRING));
        }
        bulkItems.bulkInsertAll();
        TrailingSpacesFinder.clearQueryCache();

        // Assert first and last rows
        this.assertStringValues(TrailingSpacesFinder.findByPrimaryKey(0));
        this.assertStringValues(TrailingSpacesFinder.findOneBypassCache(TrailingSpacesFinder.id().eq(0)));
        this.assertStringValues(TrailingSpacesFinder.findOneBypassCache(TrailingSpacesFinder.id().eq(1)));
        this.assertStringValues(TrailingSpacesFinder.findOneBypassCache(TrailingSpacesFinder.id().eq(count - 1)));
    }

    private void assertStringValues(TrailingSpaces item) {
        this.assertEqualsWithQuotes(item.getId(), "varCharNotNull" , TRIMMED_STRING, item.getVarCharNotNull());
        this.assertEqualsWithQuotes(item.getId(), "charNotNull"    , PADDED_STRING , item.getCharNotNull());
        this.assertEqualsWithQuotes(item.getId(), "varCharNullable", TRIMMED_STRING, item.getVarCharNullable());
        this.assertEqualsWithQuotes(item.getId(), "charNullable"   , PADDED_STRING , item.getCharNullable());
    }

    private void assertEqualsWithQuotes(int id, String description, String expectedString, String actualString) {
        Assert.assertEquals("id=" + id + ", " + description + " Strings not equal, "
                        + "expected '" + expectedString + "' (len=" + expectedString.length()
                        + ") but actual '" + actualString + "' (len=" + actualString.length() + ")",
                expectedString, actualString);
    }
}
