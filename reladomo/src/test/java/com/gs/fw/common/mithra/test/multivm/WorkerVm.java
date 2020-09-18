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

package com.gs.fw.common.mithra.test.multivm;

import com.gs.fw.common.mithra.test.util.tinyproxy.FastServletProxyFactory;
import com.gs.fw.common.mithra.test.util.tinyproxy.PspServlet;
import com.gs.fw.common.mithra.test.util.tinyproxy.ThankYouWriter;

import java.io.*;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.Iterator;
import java.util.Properties;
import org.mortbay.jetty.Server;
import org.mortbay.jetty.servlet.Context;
import org.mortbay.jetty.servlet.ServletHolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;



public class WorkerVm
{
    private static  Logger logger = LoggerFactory.getLogger(WorkerVm.class.getName());

    protected static final String JAVA_HOME = System.getProperty("java.home");
    protected static final String SYSTEM_CLASSPATH = System.getProperty("java.class.path");
    protected static final String BOOT_CLASSPATH = System.getProperty("sun.boot.class.path");
    protected static final String START_DIRECTORY = System.getProperty("user.dir");

    private static final String LOG4J_CONFIG = "log4j.configuration";

    public static final int PING_INTERVAL_MS = 10000;
    public static final int HEARTBEAT_INTERVAL_MS = 10;
    private int port;
    private int appPort1;
    private int appPort2;
    private StreamFlusher outFlusher;
    private StreamFlusher errFlusher;
    private OutputStream workerVmInput;
    private WorkerVmPinger workerPinger;
    private RemoteWorkerVm remoteWorkerVm;
    private Properties properties = new Properties();

    private Process otherVm;
    private static final String JACOCO_ARGS = "jacoco.args";
    public static final String DERBY_SERVER_DIRECTORY = "derby.system.home";
    public static final String DERBY_TMP_DIRECTORY = "derby.storage.tempDirectory";
    private static final String ALL_GOOD = "allgood";
    private static final String ALL_BAD = "allbad";

    public static Logger getLogger()
    {
        return logger;
    }

    public WorkerVm()
    {
        this.port = (int)(Math.random()*20000+10000);
        String log4jConfigValue = System.getProperty(LOG4J_CONFIG);
        if (log4jConfigValue == null)
        {
            logger.error(LOG4J_CONFIG+" was not set!");
        }
        else
        {
            properties.put(LOG4J_CONFIG, log4jConfigValue);
        }
        appendWorkerToDirectory(DERBY_SERVER_DIRECTORY);
        appendWorkerToDirectory(DERBY_TMP_DIRECTORY);
        for(Iterator it = System.getProperties().keySet().iterator(); it.hasNext();)
        {
            String key = (String) it.next();
            if (!key.startsWith("java") && !key.startsWith("user") && !key.startsWith("sun") && properties.get(key) == null)
            {
                properties.put(key, System.getProperty(key));
            }
        }
    }

    public void setApplicationPort1(int appPort1)
    {
        this.appPort1 = appPort1;
    }

    public void setApplicationPort2(int appPort2)
    {
        this.appPort2 = appPort2;
    }

    private void appendWorkerToDirectory(String key)
    {
        String derbyTmpDirectory = System.getProperty(key);
        if (derbyTmpDirectory != null)
        {
            derbyTmpDirectory += "worker";
            File derbyDir = new File(derbyTmpDirectory);
            if (!derbyDir.exists())
            {
                derbyDir.mkdirs();
            }
            properties.put(key, derbyTmpDirectory);
        }
    }

    public RemoteWorkerVm getRemoteWorkerVm()
    {
        return remoteWorkerVm;
    }

    public void startWorkerVm(Class testClass)
    {
        ArrayList cmdList = new ArrayList();
        cmdList.add(JAVA_HOME+getJavaBinary());
        if(logger.isDebugEnabled())
        {
            cmdList.add("-Xdebug");
            cmdList.add("-Xnoagent");
            cmdList.add("-Djava.compiler=NONE");
            cmdList.add("-Xrunjdwp:transport=dt_socket,server=y,suspend=n,address=15005");
        }
        String jacocoArgs = System.getProperty(JACOCO_ARGS);
        if (jacocoArgs != null)
        {
            cmdList.add(jacocoArgs);
        }
        cmdList.add("-ea");
        cmdList.add("-classpath");
        cmdList.add(SYSTEM_CLASSPATH);
        cmdList.add("-server");
        cmdList.add("-Duser.timezone=America/New_York");
        addProperties(cmdList);
        cmdList.add(this.getClass().getName());
        cmdList.add(""+port);
        cmdList.add(testClass.getName());
        cmdList.add(""+appPort1);
        cmdList.add(""+appPort2);
        String[] extraArgs = this.getExtraArgs();
        if (extraArgs != null)
        {
            for(String arg: extraArgs)
            {
                cmdList.add(arg);
            }
        }
        String[] cmdAndArgs = new String[cmdList.size()];
        cmdList.toArray(cmdAndArgs);
        try
        {
            String cmdString = "executing worker vm with";
            for(int i=0;i<cmdAndArgs.length;i++)
            {
                cmdString += " "+cmdAndArgs[i];
            }
            getLogger().debug(cmdString);
            otherVm = Runtime.getRuntime().exec(cmdAndArgs, null, new File(START_DIRECTORY));
//            this.errFlusher = new StreamFlusher(otherVm.getErrorStream(), System.err, false);
            File errFile = File.createTempFile("workervmworker", ".errlog");
            this.errFlusher = new StreamFlusher(otherVm.getErrorStream(), new FileOutputStream(errFile), false);
            this.errFlusher.start();
            waitForAllGood(otherVm.getInputStream(), errFile.getAbsolutePath(), cmdString);
            this.outFlusher = new StreamFlusher(otherVm.getInputStream(), System.out, false);
            this.outFlusher.start();
            this.workerVmInput = otherVm.getOutputStream();
            FastServletProxyFactory factory = new FastServletProxyFactory();
            remoteWorkerVm = factory.create(RemoteWorkerVm.class, "http://localhost:"+this.port+"/PspServlet");
            this.workerPinger = new WorkerVmPinger(remoteWorkerVm);
            workerPinger.start();
        }
        catch (Throwable e)
        {
            getLogger().error("could not start other vm", e);
            throwFatal("could not start other vm", e);
        }
    }

    public void throwFatal(String message, Throwable e)
    {
        try
        {
            File errFile = File.createTempFile("workervmmaster", ".errlog");
            FileOutputStream out = new FileOutputStream(errFile);
            PrintWriter writer = new PrintWriter(out);
            writer.write(message);
            while(e != null)
            {
                writeError(e, writer);
                e = e.getCause();
            }
            out.close();
        }
        catch (IOException e1)
        {
            //ignore
        }
        throw new RuntimeException(message, e);
    }

    public void writeError(Throwable e, PrintWriter writer)
    {
        writer.write(e.getClass().getName()+": "+e.getMessage());
        e.printStackTrace(writer);
    }

    private static void throwFatal(String message)
    {
        try
        {
            File errFile = File.createTempFile("workervmmaster", ".errlog");
            FileOutputStream out = new FileOutputStream(errFile);
            PrintWriter writer = new PrintWriter(out);
            writer.write(message);
            out.close();
        }
        catch (IOException e1)
        {
            //ignore
        }
        throw new RuntimeException(message);
    }

    protected String[] getExtraArgs()
    {
        return null;
    }

    private void waitForAllGood(InputStream inputStream, String errFile, String cmdString)
    {
        StringBuffer output = new StringBuffer();
        StreamSearcher goodSearcher = new StreamSearcher(ALL_GOOD);
        StreamSearcher badSearcher = new StreamSearcher(ALL_BAD);
        while(true)
        {
            try
            {
                int read = inputStream.read();
                if (read < 0)
                {
                    throwFatal("Worker VM did not start properly and closed its stream. The stream had: '"
                            + output.toString() + "' in it before closing. Also see error file: " + errFile + " started with command '" + cmdString + "'");
                }
                System.out.print((char) read);
                output.append((char) read);
                goodSearcher.consume(read);
                badSearcher.consume(read);
                if (goodSearcher.isFound())
                {
                    return;
                }
                if (badSearcher.isFound())
                {
                    throwFatal("Worker VM had trouble starting");
                }
            }
            catch (IOException e)
            {
                getLogger().error("could not write output", e);
                throwFatal("Worker VM did not start properly and had an IO exception", e);
            }
        }

    }

    private void addProperties(ArrayList cmdList)
    {
        Enumeration propertyNames = this.properties.propertyNames();
        while(propertyNames.hasMoreElements())
        {
            String name = (String) propertyNames.nextElement();
            cmdList.add("-D"+name+"="+this.properties.get(name));
        }
    }

    protected void shutdownWorkerVm()
    {
        if (this.outFlusher != null)
        {
            this.outFlusher.setDone(true);
        }
        if (this.errFlusher != null)
        {
            this.errFlusher.setDone(true);
        }
        if (this.workerPinger != null)
        {
            this.workerPinger.setDone(true);
        }
        try
        {
            if (this.workerVmInput != null)
            {
                this.workerVmInput.write('\n');
                this.workerVmInput.flush();
            }
        }
        catch (IOException e)
        {
            getLogger().error("could not communicate with Worker VM");
        }
        ThankYouWriter.getInstance().stopThankYouThread();
        if (otherVm != null)
        {
            InterrupterThread interrupter = new InterrupterThread(Thread.currentThread());
            interrupter.start();
            try
            {
                otherVm.waitFor();
                interrupter.setDone(true);
            }
            catch (InterruptedException e)
            {
                getLogger().warn("Worker VM took too long to quit");
                Thread.currentThread().interrupted();
            }
            otherVm.destroy();
        }
        ThankYouWriter.getInstance().stopThankYouThread();
        ThankYouWriter.getInstance().stopThankYouThread();
    }

    public String getJavaBinary()
    {
        if (System.getProperty("os.name").toUpperCase().contains("WINDOWS"))
        {
            return "/bin/java.exe";
        }
        return "/bin/java";
    }

    public static void main(String args[])
    {
        runWorkerVm(args, true);
    }

    protected static void runWorkerVm(String[] args, boolean runWorkerVmStartup)
    {
        Server server = new Server(Integer.parseInt(args[0]));
        Context context = new Context (server,"/",Context.SESSIONS);
        ServletHolder holder = context.addServlet(PspServlet.class, "/PspServlet");
        holder.setInitParameter("serviceInterface.RemoteWorkerVm", "com.gs.fw.common.mithra.test.multivm.RemoteWorkerVm");
        holder.setInitParameter("serviceClass.RemoteWorkerVm", "com.gs.fw.common.mithra.test.multivm.RemoteWorkerVmImpl");
        holder.setInitOrder(10);

        try
        {
            server.start();
            holder.getServlet();
            Class testClass = Class.forName(args[1]);
            MultiVmTest testCase = (MultiVmTest) testClass.newInstance();
            testCase.setApplicationPorts(Integer.parseInt(args[2]), Integer.parseInt(args[3]));
            RemoteWorkerVmImpl.setTestCase(testCase);
            if (runWorkerVmStartup) testCase.workerVmOnStartup();
            System.out.println(ALL_GOOD);
            while(true)
            {
                if (System.in.available() > 0)
                {
                    break;
                }
                Thread.sleep(100);
            }
            if (System.in.read() == -1)
            {
                System.out.println("WorkerVM System.in shutdown???!!!????");
                System.out.flush();
                Thread.sleep(1000);
                throw new IOException("EOF");
            }
            System.out.println("WorkerVm Exiting");
            RemoteWorkerVmImpl.exitWorkerVm();
            System.out.flush();
            System.exit(0);
        }
        catch (Exception e)
        {
            System.out.println(ALL_BAD);
            System.out.println("Could not start worker vm "+e.getClass().getName()+": "+e.getMessage());
            e.printStackTrace();
            System.exit(-1);
        }
        try
        {
            Thread.sleep(60000); // we sleep here to give the master vm time to ping us.
        }
        catch (InterruptedException e)
        {
            // ignore
        }
    }


    private static class StreamFlusher extends Thread
    {
        private InputStream in;
        private OutputStream out;
        private boolean done = false;
        private byte[] buf = new byte[1024];
        private boolean closeOnEnd;

        private StreamFlusher(InputStream in, OutputStream out, boolean closeOnEnd)
        {
            this.in = in;
            this.out = out;
            this.setDaemon(true);
            this.closeOnEnd = closeOnEnd;
        }

        public void setDone(boolean done)
        {
            this.done = done;
        }

        public void run()
        {
            while(!done)
            {
                try
                {
                    sleep(200);
                    int available = this.in.available();
                    while (available > 0)
                    {
                        int read = this.in.read(buf, 0, Math.min(buf.length, available));
                        if (read >= 0)
                        {
                            this.out.write(buf, 0, read);
                            available = this.in.available();
                        }
                        else
                        {
                            this.out.flush();
                            if (closeOnEnd)
                            {
                                this.out.close();
                            }
                            break;
                        }
                    }
                }
                catch (InterruptedException e)
                {
                    // nothing to do
                }
                catch (IOException e)
                {
                    getLogger().error("could not write output", e);
                }
            }
        }
    }

    private static class WorkerVmPinger extends Thread
    {
        private RemoteWorkerVm remoteWorkerVm;
        private boolean done = false;

        private WorkerVmPinger(RemoteWorkerVm remoteWorkerVm)
        {
            this.remoteWorkerVm = remoteWorkerVm;
            this.setDaemon(true);
        }

        public void setDone(boolean done)
        {
            this.done = done;
        }

        public void run()
        {
            while(!done)
            {
                try
                {
                    sleep(PING_INTERVAL_MS);
                    if (!done) remoteWorkerVm.ping();
                }
                catch (InterruptedException e)
                {
                    // ignore
                }
                catch(Throwable t)
                {
                    getLogger().error("could not ping remote vm", t);
                }
            }
        }
    }

    private static class InterrupterThread extends Thread
    {
        private Thread threadToInterrupt;
        private boolean done = false;

        private InterrupterThread(Thread threadToInterrupt)
        {
            this.threadToInterrupt = threadToInterrupt;
        }

        public synchronized void setDone(boolean done)
        {
            this.done = done;
            this.notify();
        }

        public void run()
        {
            long startTime = System.currentTimeMillis();
            while(!done)
            {
                synchronized(this)
                {
                    try
                    {
                        this.wait(100);
                    }
                    catch (InterruptedException e)
                    {
                        // nothing to do
                    }
                }
                if (System.currentTimeMillis() - startTime >= 10000)
                {
                    threadToInterrupt.interrupt();
                    break;
                }
            }
        }
    }

    private static class StreamSearcher
    {
        private String target;
        private int currentLocation = 0;
        private boolean found;

        private StreamSearcher(String target)
        {
            this.target = target;
        }

        public void consume(int read)
        {
            if (!found)
            {
                if (read == this.target.charAt(currentLocation))
                {
                    this.currentLocation++;
                    if (currentLocation == this.target.length()) found = true;
                }
                else this.currentLocation = 0;
            }
        }

        public boolean isFound()
        {
            return found;
        }
    }
}
