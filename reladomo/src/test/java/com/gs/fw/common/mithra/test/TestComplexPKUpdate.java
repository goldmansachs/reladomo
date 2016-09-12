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
import com.gs.fw.common.mithra.MithraTransaction;
import com.gs.fw.common.mithra.TransactionalCommand;
import com.gs.fw.common.mithra.finder.Operation;
import com.gs.fw.common.mithra.test.domain.*;

import java.sql.*;
import java.text.ParseException;
import java.util.Date;


public class TestComplexPKUpdate extends MithraTestAbstract
{

    public Class[] getRestrictedClassList()
    {
        return new Class[]
        {
            ExchangeRate.class,
            ExchangeRateChild.class
        };
    }

    public void testUpdateOneRow()
            throws SQLException
    {
        String currency = "EUR";
        int sourceId = 10;
        String dateString = "2004-09-30 18:30:00";
        Date date = this.convertStringToDate(dateString);

        Operation op = getOp(currency, sourceId, date);

        ExchangeRate exchangeRate = ExchangeRateFinder.findOne(op);
        assertNotNull(exchangeRate);
        double oldValue = exchangeRate.getExchangeRate();
        double newValue = oldValue + 0.21;
        exchangeRate.setExchangeRate(newValue);
        assertEquals(exchangeRate.getExchangeRate(), newValue, 0);
        checkExchangeRate(newValue, currency, sourceId, date);

    }

    private void checkExchangeRate(double newValue, String currency, int sourceId, Date date)
            throws SQLException
    {
        Connection con = this.getConnection();
        String sql = "select EXCH_RATE from FXRATE where SOURCE_I = ? AND PROD_CURRENCY_C = ? AND THRU_Z = ?";
        PreparedStatement ps = con.prepareStatement(sql);
        ps.setInt(1, sourceId);
        ps.setString(2, currency);
        ps.setTimestamp(3, new Timestamp(date.getTime()));
        ResultSet rs = ps.executeQuery();
        assertTrue(rs.next());
        assertEquals(newValue, rs.getDouble(1), 0.0001);
        rs.close();
        ps.close();
        con.close();
    }

    private void checkExchangeRateWithNullSource(double newValue, String currency, Date date)
            throws SQLException
    {
        Connection con = this.getConnection();
        String sql = "select EXCH_RATE from FXRATE where SOURCE_I IS NULL AND PROD_CURRENCY_C = ? AND THRU_Z = ?";
        PreparedStatement ps = con.prepareStatement(sql);
        ps.setString(1, currency);
        ps.setTimestamp(2, new Timestamp(date.getTime()));
        ResultSet rs = ps.executeQuery();
        assertTrue(rs.next());
        assertEquals(newValue, rs.getDouble(1), 0.0001);
        rs.close();
        ps.close();
        con.close();
    }

    private void checkExchangeRateDeleted(String currency, int sourceId, Date date)
            throws SQLException
    {
        Connection con = this.getConnection();
        String sql = "select EXCH_RATE from FXRATE where SOURCE_I = ? AND PROD_CURRENCY_C = ? AND THRU_Z = ?";
        PreparedStatement ps = con.prepareStatement(sql);
        ps.setInt(1, sourceId);
        ps.setString(2, currency);
        ps.setTimestamp(3, new Timestamp(date.getTime()));
        ResultSet rs = ps.executeQuery();
        assertFalse(rs.next());
        rs.close();
        ps.close();
        con.close();
    }

    public static Date convertStringToDate(String dateString)
    {
        java.text.SimpleDateFormat dateFormat = new java.text.SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        Date date = null;
        try
        {
            date = dateFormat.parse(dateString);
        }
        catch (ParseException e)
        {
            e.printStackTrace();
        }
        return date;
    }

    public void testUpdateMutablePkWithoutTx() throws SQLException
    {
        String currency = "EUR";
        int sourceId = 10;
        String dateString = "2004-09-30 18:30:00";
        Date date = this.convertStringToDate(dateString);

        Operation op = getOp(currency, sourceId, date);

        ExchangeRate exchangeRate = ExchangeRateFinder.findOne(op);
        assertNotNull(exchangeRate);
        double oldValue = exchangeRate.getExchangeRate();

        exchangeRate.setSource(2001);
        checkExchangeRate(oldValue, currency, 2001, date);
        assertNull(ExchangeRateFinder.findOne(op));

        int count = MithraManagerProvider.getMithraManager().getDatabaseRetrieveCount();
        Operation op2 = getOp(currency, 2001, date);

        assertNotNull(ExchangeRateFinder.findOne(op2));
        assertEquals(count, MithraManagerProvider.getMithraManager().getDatabaseRetrieveCount());
    }

    public void testUpdateMutablePkWithTx() throws SQLException
    {
        final String currency = "EUR";
        final int sourceId = 10;
        String dateString = "2004-09-30 18:30:00";
        final Date date = this.convertStringToDate(dateString);

        Operation op = getOp(currency, sourceId, date);

        final ExchangeRate exchangeRate = ExchangeRateFinder.findOne(op);
        assertNotNull(exchangeRate);
        double oldValue = exchangeRate.getExchangeRate();

        MithraManagerProvider.getMithraManager().executeTransactionalCommand(new TransactionalCommand()
        {
            public Object executeTransaction(MithraTransaction tx) throws Throwable
            {
                exchangeRate.setSource(2001);
                Operation op = getOp(currency, sourceId, date);

                assertNull(ExchangeRateFinder.findOne(op));
                int count = MithraManagerProvider.getMithraManager().getDatabaseRetrieveCount();
                Operation op2 = getOp(currency, 2001, date);

                assertNotNull(ExchangeRateFinder.findOne(op2));
                assertEquals(count, MithraManagerProvider.getMithraManager().getDatabaseRetrieveCount());
                return null;
            }
        });
        checkExchangeRate(oldValue, currency, 2001, date);
        assertNull(ExchangeRateFinder.findOne(op));

        int count = MithraManagerProvider.getMithraManager().getDatabaseRetrieveCount();
        Operation op2 = getOp(currency, 2001, date);

        assertNotNull(ExchangeRateFinder.findOne(op2));
        assertEquals(count, MithraManagerProvider.getMithraManager().getDatabaseRetrieveCount());
    }

    public void testUpdateTwoMutablePk() throws SQLException
    {
        final String currency = "EUR";
        final int sourceId = 10;
        String dateString = "2004-09-30 18:30:00";
        final Date date = this.convertStringToDate(dateString);

        Operation op = getOp(currency, sourceId, date);

        final ExchangeRate exchangeRate = ExchangeRateFinder.findOne(op);
        assertNotNull(exchangeRate);
        double oldValue = exchangeRate.getExchangeRate();

        final Timestamp newDate = new Timestamp(System.currentTimeMillis());

        MithraManagerProvider.getMithraManager().executeTransactionalCommand(new TransactionalCommand()
        {
            public Object executeTransaction(MithraTransaction tx) throws Throwable
            {
                exchangeRate.setSource(2001);
                exchangeRate.setDate(newDate);
                Operation op = getOp(currency, sourceId, date);

                assertNull(ExchangeRateFinder.findOne(op));
                int count = MithraManagerProvider.getMithraManager().getDatabaseRetrieveCount();
                Operation op2 = getOp(currency, 2001, newDate);

                assertSame(exchangeRate, ExchangeRateFinder.findOne(op2));
                assertEquals(count, MithraManagerProvider.getMithraManager().getDatabaseRetrieveCount());
                return null;
            }
        });
        checkExchangeRate(oldValue, currency, 2001, newDate);
        assertNull(ExchangeRateFinder.findOne(op));

        int count = MithraManagerProvider.getMithraManager().getDatabaseRetrieveCount();
        Operation op2 = getOp(currency, 2001, newDate);

        assertNotNull(ExchangeRateFinder.findOne(op2));
        assertEquals(count, MithraManagerProvider.getMithraManager().getDatabaseRetrieveCount());
    }

    public void testUpdateMutablePkWithTxTwice() throws SQLException
    {
        final String currency = "EUR";
        final int sourceId = 10;
        String dateString = "2004-09-30 18:30:00";
        final Date date = this.convertStringToDate(dateString);

        Operation op = getOp(currency, sourceId, date);

        final ExchangeRate exchangeRate = ExchangeRateFinder.findOne(op);
        assertNotNull(exchangeRate);
        double oldValue = exchangeRate.getExchangeRate();

        MithraManagerProvider.getMithraManager().executeTransactionalCommand(new TransactionalCommand()
        {
            public Object executeTransaction(MithraTransaction tx) throws Throwable
            {
                exchangeRate.setSource(2001);
                Operation op = getOp(currency, sourceId, date);

                assertNull(ExchangeRateFinder.findOne(op));
                int count = MithraManagerProvider.getMithraManager().getDatabaseRetrieveCount();
                Operation op2 = getOp(currency, 2001, date);

                assertSame(exchangeRate, ExchangeRateFinder.findOne(op2));
                assertEquals(count, MithraManagerProvider.getMithraManager().getDatabaseRetrieveCount());

                exchangeRate.setSource(2002);
                op = getOp(currency, 2001, date);

                assertNull(ExchangeRateFinder.findOne(op));
                count = MithraManagerProvider.getMithraManager().getDatabaseRetrieveCount();
                op2 = getOp(currency, 2002, date);

                assertSame(exchangeRate, ExchangeRateFinder.findOne(op2));
                assertEquals(count, MithraManagerProvider.getMithraManager().getDatabaseRetrieveCount());
                return null;
            }
        });
        checkExchangeRate(oldValue, currency, 2002, date);
        assertNull(ExchangeRateFinder.findOne(op));

        int count = MithraManagerProvider.getMithraManager().getDatabaseRetrieveCount();
        Operation op2 = getOp(currency, 2002, date);

        assertNotNull(ExchangeRateFinder.findOne(op2));
        assertEquals(count, MithraManagerProvider.getMithraManager().getDatabaseRetrieveCount());
    }

    public void testUpdateMutablePkWithTxTwiceCollapsed() throws SQLException
    {
        final String currency = "EUR";
        final int sourceId = 10;
        String dateString = "2004-09-30 18:30:00";
        final Date date = this.convertStringToDate(dateString);

        Operation op = getOp(currency, sourceId, date);

        final ExchangeRate exchangeRate = ExchangeRateFinder.findOne(op);
        assertNotNull(exchangeRate);
        double oldValue = exchangeRate.getExchangeRate();

        MithraManagerProvider.getMithraManager().executeTransactionalCommand(new TransactionalCommand()
        {
            public Object executeTransaction(MithraTransaction tx) throws Throwable
            {
                exchangeRate.setSource(2001);
                exchangeRate.setSource(2002);
                Operation op = getOp(currency, 2001, date);

                assertNull(ExchangeRateFinder.findOne(op));
                int count = MithraManagerProvider.getMithraManager().getDatabaseRetrieveCount();
                Operation op2 = getOp(currency, 2002, date);

                assertSame(exchangeRate, ExchangeRateFinder.findOne(op2));
                assertEquals(count, MithraManagerProvider.getMithraManager().getDatabaseRetrieveCount());
                return null;
            }
        });
        checkExchangeRate(oldValue, currency, 2002, date);
        assertNull(ExchangeRateFinder.findOne(op));

        int count = MithraManagerProvider.getMithraManager().getDatabaseRetrieveCount();
        Operation op2 = getOp(currency, 2002, date);

        assertNotNull(ExchangeRateFinder.findOne(op2));
        assertEquals(count, MithraManagerProvider.getMithraManager().getDatabaseRetrieveCount());
    }

    private Operation getOp(String currency, int sourceId, Date date)
    {
        Operation op = ExchangeRateFinder.acmapCode().eq(SOURCE_A).and(ExchangeRateFinder.currency().eq(currency));
        op = op.and(ExchangeRateFinder.date().eq(date));
        op = op.and(ExchangeRateFinder.source().eq(sourceId));
        return op;
    }

    public void testUpdateDeleteMutablePk() throws SQLException
    {
        final String currency = "EUR";
        final int sourceId = 10;
        String dateString = "2004-09-30 18:30:00";
        final Date date = this.convertStringToDate(dateString);

        Operation op = getOp(currency, sourceId, date);

        final ExchangeRate exchangeRate = ExchangeRateFinder.findOne(op);
        assertNotNull(exchangeRate);

        MithraManagerProvider.getMithraManager().executeTransactionalCommand(new TransactionalCommand()
        {
            public Object executeTransaction(MithraTransaction tx) throws Throwable
            {
                exchangeRate.setSource(2001);
                Operation op = getOp(currency, sourceId, date);

                assertNull(ExchangeRateFinder.findOne(op));
                int count = MithraManagerProvider.getMithraManager().getDatabaseRetrieveCount();
                Operation op2 = getOp(currency, 2001, date);

                assertSame(exchangeRate, ExchangeRateFinder.findOne(op2));
                assertEquals(count, MithraManagerProvider.getMithraManager().getDatabaseRetrieveCount());

                exchangeRate.delete();
                op = getOp(currency, 2001, date);

                assertNull(ExchangeRateFinder.findOne(op));
                return null;
            }
        });
        checkExchangeRateDeleted(currency, 2001, date);
        checkExchangeRateDeleted(currency, 10, date);
        assertNull(ExchangeRateFinder.findOne(op));

        Operation op2 = getOp(currency, 2001, date);

        assertNull(ExchangeRateFinder.findOne(op2));
    }

    public void testBatchUpdateMutablePkWithTx() throws SQLException
    {
        String dateString = "2004-09-30 18:30:00";
        final Date date = this.convertStringToDate(dateString);

        MithraManagerProvider.getMithraManager().executeTransactionalCommand(new TransactionalCommand()
        {
            public Object executeTransaction(MithraTransaction tx) throws Throwable
            {
                ExchangeRateList list = new ExchangeRateList(ExchangeRateFinder.acmapCode().eq(SOURCE_A).and(ExchangeRateFinder.source().eq(10)));
                list.setOrderBy(ExchangeRateFinder.currency().ascendingOrderBy());
                assertTrue(list.size() > 2);
                for(int i=0;i<list.size();i++)
                {
                    list.getExchangeRateAt(i).setSource(2000+i);
                    list.getExchangeRateAt(i).setExchangeRate(1000+i);
                }
                ExchangeRateList list2 = new ExchangeRateList(ExchangeRateFinder.acmapCode().eq(SOURCE_A).and(ExchangeRateFinder.source().eq(10)));
                assertEquals(0, list2.size());
                return null;
            }
        });
        ExchangeRateList list2 = new ExchangeRateList(ExchangeRateFinder.acmapCode().eq(SOURCE_A).and(ExchangeRateFinder.source().eq(10)));
        list2.setBypassCache(true);
        assertEquals(0, list2.size());
        ExchangeRateList list = new ExchangeRateList(ExchangeRateFinder.acmapCode().eq(SOURCE_A).and(ExchangeRateFinder.source().greaterThanEquals(2000)));
        list.setOrderBy(ExchangeRateFinder.currency().ascendingOrderBy());
        assertTrue(list.size() > 2);
        for(int i=0;i<list.size();i++)
        {
            ExchangeRate er = list.getExchangeRateAt(i);
            checkExchangeRate(1000+i, er.getCurrency(), 2000+i, date);
            er.setSource(2000+i);
        }
    }

    public void testUpdateToNullMutablePkWithoutTx() throws SQLException
    {
        String currency = "EUR";
        int sourceId = 10;
        String dateString = "2004-09-30 18:30:00";
        Date date = this.convertStringToDate(dateString);

        Operation op = getOp(currency, sourceId, date);

        ExchangeRate exchangeRate = ExchangeRateFinder.findOne(op);
        assertNotNull(exchangeRate);
        double oldValue = exchangeRate.getExchangeRate();

        exchangeRate.setSourceNull();
        checkExchangeRateWithNullSource(oldValue, currency, date);
        assertNull(ExchangeRateFinder.findOne(op));
        assertTrue(exchangeRate.isSourceNull());
        int count = MithraManagerProvider.getMithraManager().getDatabaseRetrieveCount();
        op = ExchangeRateFinder.acmapCode().eq(SOURCE_A).and(ExchangeRateFinder.currency().eq(currency));
        op = op.and(ExchangeRateFinder.date().eq(date));
        op = op.and(ExchangeRateFinder.source().isNull());


        assertNotNull(ExchangeRateFinder.findOne(op));
        assertEquals(count, MithraManagerProvider.getMithraManager().getDatabaseRetrieveCount());
    }

    public void testNullUpdateMutablePkWithTx() throws SQLException
    {
        final String currency = "EUR";
        final int sourceId = 10;
        String dateString = "2004-09-30 18:30:00";
        final Date date = this.convertStringToDate(dateString);

        Operation op = getOp(currency, sourceId, date);

        final ExchangeRate exchangeRate = ExchangeRateFinder.findOne(op);
        assertNotNull(exchangeRate);
        double oldValue = exchangeRate.getExchangeRate();

        MithraManagerProvider.getMithraManager().executeTransactionalCommand(new TransactionalCommand()
        {
            public Object executeTransaction(MithraTransaction tx) throws Throwable
            {
                exchangeRate.setSourceNull();
                Operation op = getOp(currency, sourceId, date);
                assertTrue(exchangeRate.isSourceNull());

                assertNull(ExchangeRateFinder.findOne(op));
                int count = MithraManagerProvider.getMithraManager().getDatabaseRetrieveCount();
                op = ExchangeRateFinder.acmapCode().eq(SOURCE_A).and(ExchangeRateFinder.currency().eq(currency));
                op = op.and(ExchangeRateFinder.date().eq(date));
                op = op.and(ExchangeRateFinder.source().isNull());

                assertSame(exchangeRate, ExchangeRateFinder.findOne(op));
                assertEquals(count, MithraManagerProvider.getMithraManager().getDatabaseRetrieveCount());
                return null;
            }
        });
        assertTrue(exchangeRate.isSourceNull());
        checkExchangeRateWithNullSource(oldValue, currency, date);
        assertNull(ExchangeRateFinder.findOne(op));

        int count = MithraManagerProvider.getMithraManager().getDatabaseRetrieveCount();
        op = ExchangeRateFinder.acmapCode().eq(SOURCE_A).and(ExchangeRateFinder.currency().eq(currency));
        op = op.and(ExchangeRateFinder.date().eq(date));
        op = op.and(ExchangeRateFinder.source().isNull());

        assertSame(exchangeRate, ExchangeRateFinder.findOne(op));
        assertEquals(count, MithraManagerProvider.getMithraManager().getDatabaseRetrieveCount());
    }

    public void testDetachedUpdateMutablePk() throws SQLException
    {
        String currency = "EUR";
        int sourceId = 10;
        String dateString = "2004-09-30 18:30:00";
        Date date = this.convertStringToDate(dateString);

        Operation op = getOp(currency, sourceId, date);

        ExchangeRate exchangeRate = ExchangeRateFinder.findOne(op);
        assertNotNull(exchangeRate);
        double oldValue = exchangeRate.getExchangeRate();

        ExchangeRate det = exchangeRate.getDetachedCopy();

        det.setSource(2001);
        det.copyDetachedValuesToOriginalOrInsertIfNew();

        assertEquals(2001, exchangeRate.getSource());
        checkExchangeRate(oldValue, currency, 2001, date);
        assertNull(ExchangeRateFinder.findOne(op));

        int count = MithraManagerProvider.getMithraManager().getDatabaseRetrieveCount();
        Operation op2 = getOp(currency, 2001, date);

        assertNotNull(ExchangeRateFinder.findOne(op2));
        assertEquals(count, MithraManagerProvider.getMithraManager().getDatabaseRetrieveCount());
    }

    public void testInsertMutablePkWithoutTx() throws SQLException
    {
        String currency = "EUR";
        int sourceId = 100;
        Timestamp time = new Timestamp(System.currentTimeMillis());

        Operation op = getOp(currency, sourceId, time);

        ExchangeRate exchangeRate = ExchangeRateFinder.findOne(op);
        assertNull(exchangeRate);
        exchangeRate = new ExchangeRate();
        exchangeRate.setAcmapCode(SOURCE_A);
        exchangeRate.setCurrency(currency);
        exchangeRate.setSource(sourceId);
        exchangeRate.setDate(time);
        exchangeRate.setExchangeRate(2);
        exchangeRate.insert();

        checkExchangeRate(2, currency, sourceId, time);

        int count = MithraManagerProvider.getMithraManager().getDatabaseRetrieveCount();

        assertSame(exchangeRate, ExchangeRateFinder.findOne(op));
        assertEquals(count, MithraManagerProvider.getMithraManager().getDatabaseRetrieveCount());
    }

    public void testInsertMutablePkWithTx() throws SQLException
    {
        final String currency = "EUR";
        final int sourceId = 100;
        final Timestamp time = new Timestamp(System.currentTimeMillis());

        Operation op = getOp(currency, sourceId, time);

        assertNull(ExchangeRateFinder.findOne(op));

        final ExchangeRate exchangeRate = new ExchangeRate();
        MithraManagerProvider.getMithraManager().executeTransactionalCommand(new TransactionalCommand()
        {
            public Object executeTransaction(MithraTransaction tx) throws Throwable
            {
                exchangeRate.setAcmapCode(SOURCE_A);
                exchangeRate.setCurrency(currency);
                exchangeRate.setSource(sourceId);
                exchangeRate.setDate(time);
                exchangeRate.setExchangeRate(2);
                exchangeRate.insert();
                return null;
            }
        });

        checkExchangeRate(2, currency, sourceId, time);

        int count = MithraManagerProvider.getMithraManager().getDatabaseRetrieveCount();

        assertSame(exchangeRate, ExchangeRateFinder.findOne(op));
        assertEquals(count, MithraManagerProvider.getMithraManager().getDatabaseRetrieveCount());
    }

    private void createForeignKey()
    {
        try
        {
            executeStatement("alter table FXRATE_CHILD add constraint FXRATE_FK FOREIGN KEY (PROD_CURRENCY_C,SOURCE_I,THRU_Z) REFERENCES FXRATE(PROD_CURRENCY_C,SOURCE_I,THRU_Z)");
        }
        catch (SQLException e)
        {
            throw new RuntimeException("could not create foreign key", e);
        }
    }

    private void dropForeignKey()
    {
        try
        {
            executeStatement("alter table FXRATE_CHILD drop constraint FXRATE_FK");
        }
        catch (SQLException e)
        {
            throw new RuntimeException("could not drop foreign key", e);
        }
    }

    public void testUpdateMutablePkWithCascade()
    {
        final String currency = "KRW";
        final int sourceId = 20;
        final String dateString = "2008-10-16 18:30:00";
        final Date date = this.convertStringToDate(dateString);

        MithraManagerProvider.getMithraManager().executeTransactionalCommand(new TransactionalCommand()
        {
            public Object executeTransaction(MithraTransaction tx) throws Throwable
            {
                Operation op = getOp(currency, sourceId, date);

                ExchangeRate exchangeRate = ExchangeRateFinder.findOne(op);
                exchangeRate.setSource(200);
                ExchangeRateChildList childList = exchangeRate.getChildren();
                assertEquals(2, childList.size());
                for(int i=0;i<childList.size();i++)
                {
                    assertEquals(200, childList.get(i).getSource());
                }
                return null;
            }
        });

        Operation op = getOp(currency, 200, date);

        ExchangeRate exchangeRate = ExchangeRateFinder.findOne(op);
        ExchangeRateChildList childList = exchangeRate.getChildren();
        assertEquals(2, childList.size());
        for(int i=0;i<childList.size();i++)
        {
            assertEquals(200, childList.get(i).getSource());
        }
        createForeignKey();
        try
        {
        }
        finally
        {
            dropForeignKey();
        }
    }

    public void testDetachedUpdateMutablePkWithCascade()
    {
        final String currency = "KRW";
        final int sourceId = 20;
        final String dateString = "2008-10-16 18:30:00";
        final Date date = this.convertStringToDate(dateString);

        Operation op = getOp(currency, sourceId, date);

        ExchangeRate exchangeRate = ExchangeRateFinder.findOne(op).getDetachedCopy();
        exchangeRate.setSource(200);
        ExchangeRateChildList childList = exchangeRate.getChildren();
        assertEquals(2, childList.size());
        for(int i=0;i<childList.size();i++)
        {
            assertEquals(200, childList.get(i).getSource());
        }
        exchangeRate.copyDetachedValuesToOriginalOrInsertIfNew();

        Operation op2 = getOp(currency, 200, date);

        ExchangeRate exchangeRate2 = ExchangeRateFinder.findOne(op2);
        ExchangeRateChildList childList2 = exchangeRate2.getChildren();
        assertEquals(2, childList2.size());
        for(int i=0;i<childList2.size();i++)
        {
            assertEquals(200, childList2.get(i).getSource());
        }
    }

    public void testDetachedUpateMutablePkWithDeletedChild()
    {
        createForeignKey();
        try
        {
            final String currency = "KRW";
            final int sourceId = 20;
            final String dateString = "2008-10-16 18:30:00";
            final Date date = this.convertStringToDate(dateString);

            Operation op = getOp(currency, sourceId, date);

            ExchangeRate exchangeRate = ExchangeRateFinder.findOne(op).getDetachedCopy();
            ExchangeRateChildList childList = exchangeRate.getChildren();
            assertEquals(2, childList.size());
            ExchangeRateChildList copiedChildList = childList.getNonPersistentCopy();
            childList.clear();
            exchangeRate.setSource(200);
            exchangeRate.setChildren(copiedChildList);
            for(int i=0;i<copiedChildList.size();i++)
            {
                assertEquals(200, copiedChildList.get(i).getSource());
            }
            exchangeRate.copyDetachedValuesToOriginalOrInsertIfNew();

            Operation op2 = getOp(currency, 200, date);

            ExchangeRate exchangeRate2 = ExchangeRateFinder.findOne(op2);
            ExchangeRateChildList childList2 = exchangeRate2.getChildren();
            assertEquals(2, childList2.size());
            for(int i=0;i<childList2.size();i++)
            {
                assertEquals(200, childList2.get(i).getSource());
            }
        }
        finally
        {
            dropForeignKey();
        }

    }

    public void testBatchUpdateMutablePk() throws ParseException
    {
        int setSize = 10;
        int maxLoop = 4;
        ExchangeRateList exchangeRateList = this.createNewExchangeRateList(setSize);
        exchangeRateList.bulkInsertAll();
        final Operation op = ExchangeRateFinder.acmapCode().eq("A");

        for(int k=0;k<maxLoop;k++)
        {
            final int count = k;
            long start = System.currentTimeMillis();
            MithraManagerProvider.getMithraManager().executeTransactionalCommand(
                 new TransactionalCommand()
                 {
                     public Object executeTransaction(MithraTransaction tx) throws Throwable
                     {
                         ExchangeRateList exchangeRateList = new ExchangeRateList(op);
                         exchangeRateList.deepFetch(ExchangeRateFinder.children());
                         exchangeRateList.setOrderBy(ExchangeRateFinder.currency().ascendingOrderBy());
                         for(int i = 0; i < exchangeRateList.size(); i++)
                         {
                             ExchangeRate exchangeRate  = exchangeRateList.get(i);
                             if (count > 0)
                             {
                                 assertEquals(i+count - 1, exchangeRate.getSource());
                             }
                             exchangeRate.setSource(i+count);
                         }
                         return null;
                     }
                 }
            );
//            System.out.println("took "+(System.currentTimeMillis() - start)+" ms");
        }

        exchangeRateList = new ExchangeRateList(op);
        exchangeRateList.setOrderBy(ExchangeRateFinder.currency().ascendingOrderBy());
        exchangeRateList.setBypassCache(true);
        assertTrue(exchangeRateList.size() > 0);
        for(int i = 0; i < exchangeRateList.size(); i++)
        {
            ExchangeRate exchangeRate  = exchangeRateList.get(i);
            assertEquals(i+maxLoop-1, exchangeRate.getSource());
        }
    }

    private ExchangeRateList createNewExchangeRateList(int setSize) throws ParseException
    {
        ExchangeRateList result = new ExchangeRateList();
        Timestamp now = new Timestamp(timestampFormat.parse("2008-01-01 00:00:00.000").getTime());
        for(int i=0;i<setSize;i++)
        {
            ExchangeRate b = new ExchangeRate();
            b.setAcmapCode("A");
            b.setSourceNull();
            b.setCurrency(""+i);
            b.setDate(now);
            result.add(b);
        }
        return result;
    }
}
