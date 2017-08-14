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

package com.gs.fw.common.mithra.test.vendor;


import java.sql.*;
import java.util.Calendar;
import java.util.Properties;
import java.util.TimeZone;

// test passes with old version of drivers 2.0, from here:
// https://www.microsoft.com/en-us/download/details.aspx?id=2505
// test fails with newer 4.x and 6.x series drivers, including the open source 6.2.1
// test also passes with jtds-jdbc on MS SQL Server, and it passes on at least seven other databases

public class SetTimestampWithUtc
{
    private static final String DATABASE_URL = "jdbc:sqlserver://fill_in_your_hostname:fill_in_your_port;databaseName=fill_in_your_db";
    private static final String USER = "fill_in_your_user";
    private static final String PASSWORD = "fill_in_your_password";

    public static final String DRIVER_NAME = "com.microsoft.sqlserver.jdbc.SQLServerDriver";
//    public static final String DRIVER_NAME = "net.sourceforge.jtds.jdbc.Driver";

    public static void main(String[] args) throws Exception
    {
        Connection con = createConnection();
        Statement stm = con.createStatement();
        stm.executeUpdate("create table TEST_TIMESTAMP_CAL ( ID integer not null, TIMESTAMP_COL_NONE datetime not null )");
        stm.close();

        stm = con.createStatement();
        stm.executeUpdate("INSERT INTO TEST_TIMESTAMP_CAL (ID,TIMESTAMP_COL_NONE) values (1,'2007-01-01 01:01:01.999')");
        stm.close();

        stm = con.createStatement();
        ResultSet rs = stm.executeQuery("select count(1) from TEST_TIMESTAMP_CAL where TIMESTAMP_COL_NONE = '2007-01-01 01:01:01.999'");
        int firstCount = assertOneRow(rs);
        stm.close();

        Calendar c = Calendar.getInstance();
        c.setTimeZone(TimeZone.getTimeZone("UTC"));

        PreparedStatement ps = con.prepareStatement("select count(1) from TEST_TIMESTAMP_CAL where TIMESTAMP_COL_NONE = ?");
        ps.setTimestamp(1, new Timestamp(1167613261999L), c); //  1167613261999L is '2007-01-01 01:01:01.999 EST' in UTC
        rs = ps.executeQuery();
        int secondCount = assertOneRow(rs);
        ps.close();

        stm = con.createStatement();
        stm.executeUpdate("drop table TEST_TIMESTAMP_CAL");
        stm.close();
        con.close();

        if (firstCount != 1)
        {
            System.out.println("hardcoded timestamp failed! Expecting 1, but got " + firstCount);
        }
        else if (secondCount != 1)
        {
            System.out.println("preparedStatement.setTimestamp with calendar failed! Expecting 1, but got " + secondCount);
        }
        else
        {
            System.out.println("All good.");
        }
    }

    private static Connection createConnection()
            throws ClassNotFoundException, SQLException
    {
        Class.forName(DRIVER_NAME);
        Driver driver = DriverManager.getDriver(DATABASE_URL);
        Properties props = new Properties();
        props.put("user", USER);
        props.put("password", PASSWORD);

        Connection con = driver.connect(DATABASE_URL, props);
//        con.setAutoCommit(false);
//        con.setTransactionIsolation(Connection.TRANSACTION_SERIALIZABLE);
        System.out.println("Using driver: " + con.getMetaData().getDriverName() + " " + con.getMetaData().getDriverVersion());
        return con;
    }

    private static int assertOneRow(ResultSet rs) throws SQLException
    {
        if (!rs.next())
        {
            return -1;
        }
        int count = rs.getInt(1);
        rs.close();
        return count;
    }
}
