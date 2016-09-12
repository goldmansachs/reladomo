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

import com.gs.fw.common.mithra.database.SqlLogSnooper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class LoadingTaskMonitor
{
    static private Logger logger = LoggerFactory.getLogger(LoadingTaskMonitor.class);
    private final String classToLoad;
    private final String threadPoolName;
    private final Object sourceAttribute;

    private ConfigValues configValues;

    private String sqlLog;
    private long startTime = 0L;
    private long finishTime = 0L;
    private LoadingTaskRunner.State state;
    private int loadedSize;

    public LoadingTaskMonitor(LoadingTask loadingTask, String threadPoolName)
    {
        this.classToLoad = loadingTask.getClassName();
        this.threadPoolName = threadPoolName;
        this.sourceAttribute = loadingTask.getSourceAttribute();
    }

    public void setConfigValues(ConfigValues values)
    {
        this.configValues = values;
    }

    public void startMonitoring(LoadingTaskRunner.State state)
    {
        this.startTime = System.currentTimeMillis();
        if (this.configValues.isCaptureTaskSQLs())
        {
            SqlLogSnooper.startSqlSnooping();
        }
        this.state = state;
    }

    public void finishMonitoring(LoadingTask loadingTask, int loadedSize, LoadingTaskRunner.State state)
    {
        this.finishTime = System.currentTimeMillis();
        this.state = state;
        this.loadedSize = loadedSize;

        if (this.configValues.isCaptureTaskSQLs())
        {
            this.sqlLog = SqlLogSnooper.completeSqlSnooping();
            this.reportSlowSQL();
        }
    }

    protected void reportSlowSQL()
    {
        if (logger.isWarnEnabled())
        {
            long elapsedTime = this.finishTime - this.startTime;
            if (elapsedTime > this.configValues.getReportedSlowSQLTime() &&
                    (this.loadedSize == 0 || elapsedTime / (long) this.loadedSize > this.configValues.getReportedSlowSQLPerRowTime()))
            {
                StringBuilder builder = new StringBuilder("Encountered a potentially really bad SQL (class ");
                builder.append(this.getClassToLoad()).append(", ").append(this.loadedSize).append(" recs in ");
                builder.append(elapsedTime).append(" ms with SQL:\n");
                builder.append(this.getSql());

                logger.warn(builder.toString());
            }
        }
    }

    public long getStartTime()
    {
        return this.startTime;
    }

    public long getFinishTime()
    {
        return this.finishTime;
    }

    public LoadingTaskRunner.State getState()
    {
        return state;
    }

    public String getSql()
    {
        return this.sqlLog;
    }

    public String getClassToLoad()
    {
        return classToLoad;
    }

    public Object getSourceAttribute()
    {
        return sourceAttribute;
    }

    public String getThreadPoolName()
    {
        return threadPoolName;
    }

    public int getLoadedSize()
    {
        return loadedSize;
    }

    public String toString()
    {
        StringBuilder builder = new StringBuilder();
        builder.append(this.getClassToLoad())
                .append("|").append(this.getSourceAttribute())
                .append("|").append(this.getThreadPoolName());
        builder.append("|").append(this.state);
        if (this.state == LoadingTaskRunner.State.COMPLETED)
        {
            builder.append(" ").append(this.getLoadedSize());
            builder.append(" records ").append(this.finishTime - this.startTime).append(" ms");
        }
        return builder.toString();
    }
}
