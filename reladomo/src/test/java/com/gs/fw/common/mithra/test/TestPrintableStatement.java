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

import com.gs.fw.common.mithra.finder.PrintablePreparedStatement;
import junit.framework.TestCase;

import java.sql.SQLException;
import java.sql.Timestamp;

public class TestPrintableStatement
extends TestCase
{

    public void testPrintablePreparedStatement() throws SQLException
    {
        String initialStatment = "select ";
        for(int i=0;i<300;i++)
        {
            initialStatment += "?, ";
        }
        try
        {
            Thread.sleep(1000); // give the system time to "rest"
        }
        catch (InterruptedException e)
        {
            // whatever
        }
        long startTime = System.currentTimeMillis();
        Timestamp timestamp = new Timestamp(startTime);
        for(int k=0;k<10000;k++)
        {
            PrintablePreparedStatement pps = new PrintablePreparedStatement(initialStatment);
            for(int i=0;i<100;i++)
            {
                pps.setInt(i+1, i+1000);
            }
            for(int i=0;i<100;i++)
            {
                pps.setString(i+100+1, "some string");
            }
            for(int i=0;i<100;i++)
            {
                pps.setTimestamp(i+200+1, timestamp);
            }
            pps.getPrintableStatement();
        }
        double totalTime = System.currentTimeMillis() - startTime;
        System.out.println("total time for printable statement "+totalTime);
    }

    public void testPrintablePreparedStatementWithFewParameters() throws SQLException
    {
        String initialStatment = "select some really long string that is typical of pre-where clause select statements with lots of columns and tables and kitchen sinks ";
        for(int i=0;i<9;i++)
        {
            initialStatment += "?, ";
        }
        try
        {
            Thread.sleep(1000); // give the system time to "rest"
        }
        catch (InterruptedException e)
        {
            // whatever
        }
        long startTime = System.currentTimeMillis();
        Timestamp timestamp = new Timestamp(startTime);
        for(int k=0;k<100000;k++)
        {
            PrintablePreparedStatement pps = new PrintablePreparedStatement(initialStatment);
            for(int i=0;i<3;i++)
            {
                pps.setInt(i+1, i+1000);
            }
            for(int i=0;i<3;i++)
            {
                pps.setString(i+3+1, "some string");
            }
            for(int i=0;i<3;i++)
            {
                pps.setTimestamp(i+6+1, timestamp);
            }
            pps.getPrintableStatement();
        }
        double totalTime = System.currentTimeMillis() - startTime;
        System.out.println("total time for printable statement "+totalTime);
    }


}
