
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

import com.gs.fw.common.mithra.MithraTransaction;
import com.gs.fw.common.mithra.behavior.txparticipation.ReadCacheUpdateCausesRefreshAndLockTxParticipationMode;
import com.gs.fw.common.mithra.finder.Operation;
import com.gs.fw.common.mithra.test.domain.NonAuditedBalanceInterface;
import com.gs.fw.common.mithra.test.domain.NonAuditedBalanceListInterface;
import com.gs.fw.common.mithra.test.domain.NonAuditedBalanceNull;
import com.gs.fw.common.mithra.test.domain.NonAuditedBalanceNullFinder;
import com.gs.fw.common.mithra.test.domain.TestAgeBalanceSheetRunRateInterface;
import com.gs.fw.common.mithra.test.domain.TestAgeBalanceSheetRunRateNull;
import com.gs.fw.common.mithra.test.domain.TestAgeBalanceSheetRunRateNullFinder;
import com.gs.fw.common.mithra.util.NullDataTimestamp;
import org.eclipse.collections.impl.set.mutable.primitive.IntHashSet;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;

public class TestDatedNonAuditedNull extends TestDatedNonAudited
{

    public Class[] getRestrictedClassList()
    {
        return new Class[]
        {
            NonAuditedBalanceNull.class,
            TestAgeBalanceSheetRunRateNull.class
        };
    }

    public void checkDatedNonAuditedInfinityRow(int balanceId, double quantity, Timestamp businessDate) throws SQLException
    {
        Connection con = this.getConnection();
        String sql = "select POS_QUANTITY_M, FROM_Z from NON_AUDITED_BALANCE_NULL where BALANCE_ID = ? and " +
                "THRU_Z is null";
        PreparedStatement ps = con.prepareStatement(sql);
        ps.setInt(1, balanceId);
        ResultSet rs = ps.executeQuery();
        assertTrue(rs.next());
        double resultQuantity = rs.getDouble(1);
        Timestamp resultBusinessDate = rs.getTimestamp(2);
        boolean hasMoreResults = rs.next();
        rs.close();
        ps.close();
        con.close();
        assertTrue(quantity == resultQuantity);
        assertEquals(businessDate, resultBusinessDate);
        assertFalse(hasMoreResults);
    }

    public int checkDatedNonAuditedRowCounts(int balanceId) throws SQLException
    {
        Connection con = this.getConnection();
        String sql = "select count(*) from NON_AUDITED_BALANCE_NULL where BALANCE_ID = ?";
        PreparedStatement ps = con.prepareStatement(sql);
        ps.setInt(1, balanceId);
        ResultSet rs = ps.executeQuery();
        assertTrue(rs.next());

        int counts = rs.getInt(1);
        assertFalse(rs.next());
        rs.close();
        ps.close();
        con.close();

        return counts;
    }

    public void checkDatedNonAuditedTerminated(int balanceId)
            throws SQLException
    {
        Connection con = this.getConnection();
        String sql = "select count(*) from NON_AUDITED_BALANCE_NULL where BALANCE_ID = ? and " +
                "THRU_Z is null";
        PreparedStatement ps = con.prepareStatement(sql);
        ps.setInt(1, balanceId);
        ResultSet rs = ps.executeQuery();
        assertTrue(rs.next());
        int count = rs.getInt(1);
        assertFalse(rs.next());
        rs.close();
        ps.close();
        con.close();
        assertEquals(0, count);
    }

    protected void removeLockNonAuditedBalance(MithraTransaction tx)
    {
        NonAuditedBalanceNullFinder.setTransactionModeDangerousNoLocking(tx);
    }

    protected void clearNonAuditedBalanceCache()
    {
        NonAuditedBalanceNullFinder.clearQueryCache();
    }

    protected void changeParticipationMode(MithraTransaction tx)
    {
        tx.setTxParticipationMode(NonAuditedBalanceNullFinder.getMithraObjectPortal(), ReadCacheUpdateCausesRefreshAndLockTxParticipationMode.getInstance());
    }

    protected void findOneNonAuditedBalance(int i, Timestamp date)
    {
        NonAuditedBalanceNullFinder.findOne(forId(i, date));
    }


    protected NonAuditedBalanceListInterface buildInSetQuery(IntHashSet balanceIds, Timestamp businessDate)
    {
        return  NonAuditedBalanceNullFinder.findMany(NonAuditedBalanceNullFinder.acmapCode().eq("A")
                        .and(NonAuditedBalanceNullFinder.balanceId().in(balanceIds))
                        .and(NonAuditedBalanceNullFinder.businessDate().eq(businessDate)));
    }

    protected NonAuditedBalanceInterface queryNonAuditedBalancFinder(int balanceId, Timestamp businessDate)
    {
        return NonAuditedBalanceNullFinder.findOne(NonAuditedBalanceNullFinder.acmapCode().eq("A")
                 .and(NonAuditedBalanceNullFinder.balanceId().greaterThan(balanceId - 1))
                 .and(NonAuditedBalanceNullFinder.balanceId().lessThan(balanceId + 1))
                            .and(NonAuditedBalanceNullFinder.businessDate().eq(businessDate)));
    }

    protected TestAgeBalanceSheetRunRateInterface findTestAgeBalanceSheetRunRate(Timestamp businessDate)
    {
        return TestAgeBalanceSheetRunRateNullFinder.findOne(
                TestAgeBalanceSheetRunRateNullFinder.businessDate().eq(businessDate)
                .and(TestAgeBalanceSheetRunRateNullFinder.tradingdeskLevelTypeId().eq(10))
                .and(TestAgeBalanceSheetRunRateNullFinder.tradingDeskorDeskHeadId().eq(1000)));
    }




    public NonAuditedBalanceInterface buildNonAuditedBalance(Timestamp businessDate)
    {
        return new NonAuditedBalanceNull(businessDate);
    }


    protected Operation forId(int id, Timestamp date)
    {
        return NonAuditedBalanceNullFinder.acmapCode().eq("A").and(NonAuditedBalanceNullFinder.balanceId().greaterThanEquals(id).and(
                NonAuditedBalanceNullFinder.businessDate().eq(date)));
    }


    protected NonAuditedBalanceInterface findNonAuditedBalanceForBusinessDate(int balanceId, Timestamp businessDate)
    {
        return NonAuditedBalanceNullFinder.findOne(NonAuditedBalanceNullFinder.acmapCode().eq("A")
                            .and(NonAuditedBalanceNullFinder.balanceId().eq(balanceId))
                            .and(NonAuditedBalanceNullFinder.businessDate().eq(businessDate)));
    }

    public Timestamp getInifinity()
    {
        return NullDataTimestamp.getInstance();
    }


}
