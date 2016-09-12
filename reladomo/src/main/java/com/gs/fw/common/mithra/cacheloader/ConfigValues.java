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

package com.gs.fw.common.mithra.cacheloader;


public class ConfigValues
{
    private long reportedSlowSQLTime = 1000;  // milliseconds
    private long reportedSlowSQLPerRowTime = 1000; // milliseconds
    private boolean captureLoadingTaskDetails = false;

    private int threadsPerDbServer = 10;  // threads
    private double syslogCheckThreshold = 45.0;  // percent
    private long syslogCheckMaxWait = 1200 * 1000; // milliseconds

    public ConfigValues()
    {
    }

    public ConfigValues(
            int reportedSlowSQLSec, int reportedSlowSQLPerRowSec, boolean captureLoadingTaskDetails,
            int threadsPerDbServer, double syslogCheckThreshold, long syslogCheckMaxWait
    )
    {
        this.reportedSlowSQLTime = reportedSlowSQLSec * 1000L;
        this.reportedSlowSQLPerRowTime = reportedSlowSQLPerRowSec * 1000L;
        this.captureLoadingTaskDetails = captureLoadingTaskDetails;
        this.threadsPerDbServer = threadsPerDbServer;
        this.syslogCheckThreshold = syslogCheckThreshold;
        this.syslogCheckMaxWait = syslogCheckMaxWait * 1000L;
    }

    public long getReportedSlowSQLTime()
    {
        return reportedSlowSQLTime;
    }

    public long getReportedSlowSQLPerRowTime()
    {
        return reportedSlowSQLPerRowTime;
    }

    public boolean isCaptureLoadingTaskDetails()
    {
        return captureLoadingTaskDetails;
    }

    public boolean isCaptureTaskSQLs()
    {
        return reportedSlowSQLTime >= 0 || isCaptureLoadingTaskDetails();
    }

    public int getThreadsPerDbServer()
    {
        return threadsPerDbServer;
    }

    public double getSyslogCheckThreshold()
    {
        return syslogCheckThreshold;
    }

    public long getSyslogCheckMaxWait()
    {
        return syslogCheckMaxWait;
    }
}
