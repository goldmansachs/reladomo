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

import java.sql.SQLException;
import java.sql.Timestamp;



public interface TestDatedNonAuditedDatabaseChecker
{

    public void checkDatedNonAuditedTerminated(int balance) throws SQLException;
    public void checkDatedNonAuditedInfinityRow(int balanceId, double quantity, Timestamp businessDate) throws SQLException;
    public int checkDatedNonAuditedRowCounts(int balanceId) throws SQLException;
}
