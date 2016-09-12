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

import junit.framework.AssertionFailedError;
import junit.framework.Test;
import junit.framework.TestCase;
import org.apache.log4j.Logger;
import org.apache.tools.ant.BuildException;
import org.apache.tools.ant.taskdefs.optional.junit.JUnitResultFormatter;
import org.apache.tools.ant.taskdefs.optional.junit.JUnitTest;

import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintStream;



public class JunitFormatter implements JUnitResultFormatter
{
    static private Logger logger;

    private static PrintStream originalOut;

    private OutputStream out;
    private static SettablePrintStream printStream;
    private long lastMessageTime;
    private int tests;
    private int failures;
    private int errors;
    private String suiteName;

    static
    {
        originalOut = System.out;
        printStream = new SettablePrintStream(originalOut);
        System.setErr(printStream);
        System.setOut(printStream);
        logger = org.apache.log4j.Logger.getLogger(JunitFormatter.class.getName());
    }
    
    public JunitFormatter()
    {
    }

    public void startTest(Test test)
    {
        tests++;
        if (System.currentTimeMillis() - lastMessageTime > 30000)
        {
            lastMessageTime = System.currentTimeMillis();
            originalOut.println("Running "+suiteName+" So far, Tests "+tests+" Failures: "+failures+" Errors: "+errors);
        }
        originalOut.flush();
        logger.info("Start "+ getName(test));
        flush();
    }

    private String getName(Test test)
    {
        String name = test.getClass().getSimpleName();
        if (test instanceof TestCase)
        {
            name += "."+((TestCase)test).getName();
        }
        return name;
    }

    public void endTest(Test test)
    {
        logger.info("End "+ getName(test));
        flush();
    }

    public void setOutput(OutputStream out)
    {
        this.out = out;
        this.printStream.setPrintStream(new PrintStream(out));
//        WriterAppender appender = new WriterAppender(new PatternLayout("%d %-5p [%t] %-17c{2} %3x - %m%n"), out);
//        org.apache.log4j.Logger.getRootLogger().addAppender(appender);
    }

    private void flush()
    {
        if (this.out != null)
        {
            try
            {
                this.printStream.flush();
                this.out.flush();
            }
            catch (IOException e)
            {
                System.err.println("flush failed "+e.getClass().getName()+": "+e.getMessage());
            }
        }
    }

    public void addError(Test test, Throwable t)
    {
        errors++;
        originalOut.println("\n"+getName(test)+" failed with error");
        logger.error(getName(test)+" failed with error", t);
        flush();
    }

    public void addFailure(Test test, AssertionFailedError t)
    {
        failures++;
        originalOut.println("\n"+getName(test)+" failed");
        logger.error(getName(test)+" failed", t);
        flush();
    }

    public void addFailure(Test test, Throwable t)
    {
        failures++;
        originalOut.println("\n"+getName(test)+" failed");
        logger.error(getName(test)+" failed", t);
        flush();
    }

    public void endTestSuite(JUnitTest suite) throws BuildException
    {
        originalOut.println("End Suite "+suiteName+" Tests: "+tests+" Failures: "+failures+" Errors: "+errors);
        flush();
    }

    public void setSystemError(String err)
    {
        flush();
    }

    public void setSystemOutput(String out)
    {
        flush();
    }

    public void startTestSuite(JUnitTest suite)
    {
        failures = 0;
        errors = 0;
        tests = 0;
        suiteName = suite.getName();
        if (suiteName.lastIndexOf('.') > 0)
        {
            suiteName = suiteName.substring(suiteName.lastIndexOf('.')+1);
        }
        suiteName += "/"+System.getProperty("mithra.xml.config");
        originalOut.println("Start Suite "+ suiteName);
        lastMessageTime = System.currentTimeMillis();
        originalOut.flush();
        flush();
    }

    private static class SettablePrintStream extends PrintStream
    {
        private PrintStream printStream;

        public SettablePrintStream(PrintStream printStream)
        {
            super(printStream);
            this.printStream = printStream;
        }

        public void setPrintStream(PrintStream printStream)
        {
            this.printStream = printStream;
        }

        public void print(boolean b)
        {
            printStream.print(b);
        }

        public void print(char c)
        {
            printStream.print(c);
        }

        public void print(double d)
        {
            printStream.print(d);
        }

        public void print(float f)
        {
            printStream.print(f);
        }

        public void print(int i)
        {
            printStream.print(i);
        }

        public void print(long l)
        {
            printStream.print(l);
        }

        public void print(Object obj)
        {
            printStream.print(obj);
        }

        public void print(char s[])
        {
            printStream.print(s);
        }

        public void print(String s)
        {
            printStream.print(s);
        }

        public void println()
        {
            printStream.println();
        }

        public void println(boolean x)
        {
            this.print(x);
            this.println();
        }

        public void println(char x)
        {
            this.print(x);
            this.println();
        }

        public void println(char x[])
        {
            this.print(x);
            this.println();
        }

        public void println(double x)
        {
            this.print(x);
            this.println();
        }

        public void println(float x)
        {
            this.print(x);
            this.println();
        }

        public void println(int x)
        {
            this.print(x);
            this.println();
        }

        public void println(long x)
        {
            this.print(x);
            this.println();
        }

        public void println(Object x)
        {
            this.print(x);
            this.println();
        }

        public void println(String x)
        {
            this.print(x);
            this.println();
        }

        public void write(int b)
        {
            printStream.write(b);
        }

        public void write(byte buf[], int off, int len)
        {
            printStream.write(buf, off, len);
        }
    }
}
