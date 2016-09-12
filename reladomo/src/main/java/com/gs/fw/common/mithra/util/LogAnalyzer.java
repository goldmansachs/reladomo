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

package com.gs.fw.common.mithra.util;

import com.gs.collections.api.block.HashingStrategy;
import com.gs.collections.impl.map.strategy.mutable.UnifiedMapWithHashingStrategy;

import java.io.*;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.text.ParseException;



public class LogAnalyzer
{
    public static final DateFormat DATE_FORMAT = new SimpleDateFormat("dd MMM yyyy HH:mm:ss,SSS");

    public static final DateFormat ALTERNATE_FORMAT = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss,SSS");
    private static HashMap<String, AllInfo> classToInfoMap = new HashMap<String, AllInfo>();
    private static final int QUERY = 0;
    private static final int  FIND_DATE_RANGE = 1;
    private static final int  UPDATE = 2;
    private static final int  MULTI_UPDATE = 3;
    private static final int  REFRESH = 4;
    private static final int  BULK_INSERT = 5;
    private static final int  MULTI_INSERT = 6;
    private static final int  BATCH_INSERT = 7;
    private static final int  INSERT = 8;


    public static void main(String[] args)
    {
        if (args.length < 1)
        {
            usage();
            System.exit(-1);
        }
        File logFile = new File(args[0]);
        if (!logFile.exists())
        {
            System.err.println("Could not find log file "+args[0]);
            System.exit(-2);
        }
        LineNumberReader lineNumberReader = null;
        try
        {
            lineNumberReader = new LineNumberReader(new FileReader(logFile));
            String line = null;
            UnifiedMapWithHashingStrategy lastStatement = new UnifiedMapWithHashingStrategy(new ThreadHashStrategy());
            while((line = lineNumberReader.readLine()) != null)
            {
                if (line.length() > 20 && Character.isDigit(line.charAt(0)) && line.indexOf('[') > 20 && line.indexOf(']') > 0)
                {
                    LogLine logLine = new LogLine(line);
                    LogLine lastLogLine = (LogLine) lastStatement.remove(logLine);
                    if (lastLogLine != null)
                    {
                        if (lastLogLine.updateInfo(logLine))
                        {
                            System.out.println(lastLogLine);
                        }
                    }
                    if (line.indexOf(".sqllogs.") > 0)
                    {
                        lastStatement.put(logLine, logLine);
                        if (line.indexOf("find with") > 0)
                        {
                            logLine.setLogLineType(QUERY);
                        }
                        else if (line.indexOf("find datarange") > 0)
                        {
                            logLine.setLogLineType(FIND_DATE_RANGE);
                        }
                        else if (line.indexOf("update with") > 0)
                        {
                            logLine.setLogLineType(UPDATE);
                        }
                        else if (line.indexOf("multi updating with") > 0)
                        {
                            logLine.setLogLineType(MULTI_UPDATE);
                        }
                        else if (line.indexOf("refresh with") > 0)
                        {
                            logLine.setLogLineType(REFRESH);
                        }
                        else if (line.indexOf("Batch inserting with temp") > 0)
                        {
                            logLine.setLogLineType(BULK_INSERT);
                        }
                        else if (line.indexOf("multi inserting with") > 0)
                        {
                            logLine.setLogLineType(MULTI_INSERT);
                        }
                        else if (line.indexOf("batch inserting with") > 0)
                        {
                            logLine.setLogLineType(BATCH_INSERT);
                        }
                        else if (line.indexOf("insert with") > 0)
                        {
                            logLine.setLogLineType(INSERT);
                        }
                    }
                }
            }
            System.out.println("Class, "+AllInfo.getHeader());
            Iterator<String> classIterator = classToInfoMap.keySet().iterator();
            while(classIterator.hasNext())
            {
                String className = classIterator.next();
                System.out.println(className+","+classToInfoMap.get(className));
            }
        }
        catch (Exception e)
        {
            e.printStackTrace();
        }
        finally
        {
            if (lineNumberReader != null)
            {
                try
                {
                    lineNumberReader.close();
                }
                catch (IOException e)
                {
                    //ignore
                }
            }
        }
    }

    private static AllInfo getOrCreateAllInfo(LogLine resultLogLine)
    {
        AllInfo info = classToInfoMap.get(resultLogLine.getClassName());
        if (info == null)
        {
            info = new AllInfo();
            classToInfoMap.put(resultLogLine.getClassName(), info);
        }
        return info;
    }

    private static void usage()
    {
        System.out.println("you must provide a filename. Usage: LogAnalyzer <logfile>");
    }

    private static class LogLine
    {
        private Date date;
        private String threadName;
        private String level;
        private String className;
        private String logMessage;
        private int logLineType = -1;
        private LogLine nextLine;

        public LogLine(String line) throws ParseException
        {
            int threadNameStart = line.indexOf('[');
            int threadNameEnd = line.indexOf(']');
            this.date = parseDate(line.substring(0, threadNameStart));
            this.threadName = line.substring(threadNameStart+1, threadNameEnd);
            int levelEndIndex = line.indexOf(' ', threadNameEnd+2);
            this.level = line.substring(threadNameEnd+2, levelEndIndex).trim();
            int classEndIndex = line.indexOf(' ', levelEndIndex+2);
            this.className = line.substring(levelEndIndex, classEndIndex).trim();
            if (this.className.indexOf('.') > 0)
            {
                this.className = this.className.substring(this.className.lastIndexOf('.')+1, this.className.length());
            }
            this.logMessage = line.substring(classEndIndex, line.length());
        }

        private Date parseDate(String dateStr) throws ParseException
        {
            try
            {
                return DATE_FORMAT.parse(dateStr);
            }
            catch(ParseException e)
            {
                // ok, we'll try it again
            }
            return ALTERNATE_FORMAT.parse(dateStr);
        }

        public Date getDate()
        {
            return date;
        }

        public String getThreadName()
        {
            return threadName;
        }

        public void setLogLineType(int logLineType)
        {
            this.logLineType = logLineType;
        }

        public String getClassName()
        {
            return className;
        }
        public String toString()
        {
            return DATE_FORMAT.format(this.date)+" ["+this.threadName+"] "+this.level+" "+this.className+" "+this.logMessage;
        }

        public int getTotalTime()
        {
            long start = this.getDate().getTime();
            long end = this.nextLine.getDate().getTime();
            long totalTime = end - start;
            return (int) totalTime;
        }

        public boolean updateInfo(LogLine logLine)
        {
            if (this.logLineType >= 0)
            {
                this.nextLine = logLine;
                AllInfo allInfo = getOrCreateAllInfo(this);
                if (allInfo.infos[logLineType] == null)
                {
                    allInfo.infos[logLineType] = new DetailedInfo(this.getTotalTime());
                    return true;
                }
                else
                {
                    return allInfo.infos[logLineType].addTime(this.getTotalTime());
                }
            }
            return false;
        }
    }

    private static class AllInfo
    {
        public DetailedInfo[] infos = new DetailedInfo[9];

        public static String getHeader()
        {
            return "Total time"+","+DetailedInfo.getHeader("query")+","+DetailedInfo.getHeader("find daterange")+","+
                    DetailedInfo.getHeader("update")+","+DetailedInfo.getHeader("multi update")+","+
                    DetailedInfo.getHeader("refresh")+","+DetailedInfo.getHeader("bulk insert")+","+
                    DetailedInfo.getHeader("multi insert")+","+DetailedInfo.getHeader("batch insert")+","+
                    DetailedInfo.getHeader("insert");
        }

        @Override
        public String toString()
        {
            int totalTime = 0;
            for(DetailedInfo info: infos)
            {
                totalTime += info == null ? 0 : info.getTotalTime();
            }
            String result = totalTime+",";
            for(int i=0;i<infos.length;i++)
            {
                if (i > 0)
                {
                    result = result +",";
                }
                if (infos[i] == null)
                {
                    result += "0,0,0";
                }
                else
                {
                    result += infos[i].toString();
                }
            }
            return result;
        }
    }

    private static class DetailedInfo
    {
        private int totalTime;
        private int numberOfExecutions;
        private int longestExecution;

        public DetailedInfo(int time)
        {
            this.totalTime = time;
            this.numberOfExecutions = 1;
            this.longestExecution = time;
        }

        public boolean addTime(int time)
        {
            this.totalTime += time;
            this.numberOfExecutions++;
            if (this.longestExecution < time)
            {
                this.longestExecution = time;
                return true;
            }
            return false;
        }

        public String toString()
        {
            return this.totalTime+","+this.numberOfExecutions +","+this.longestExecution;
        }

        public static String getHeader(String type)
        {
            return type+": time, "+type+": Number of executions, "+type+": Longest execution";
        }

        public int getTotalTime()
        {
            return totalTime;
        }
    }

    private static class ThreadHashStrategy implements HashingStrategy
    {
        public int computeHashCode(Object o)
        {
            LogLine line = (LogLine) o;
            return line.getThreadName().hashCode();
        }

        public boolean equals(Object o, Object o1)
        {
            LogLine left = (LogLine) o;
            LogLine right = (LogLine) o1;
            return left.getThreadName().equals(right.getThreadName());
        }
    }
}
