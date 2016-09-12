
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

package com.gs.fw.common.mithra.util.execute;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.List;


public class Execute
{
    private static final Logger logger = LoggerFactory.getLogger(Execute.class.getName());

    private List command;
    private StreamHandler handler;
    private boolean terminateOnJvmExit;
    private File workingDirectory;

    public void setCommand(List command)
    {
        this.command = command;
    }

    public void setStreamHandler(StreamHandler handler)
    {
        this.handler = handler;
    }

    /**
     * Sets whether or not the process should be terminated when the JVM exits.
     * @param flag <code>true</code> if the process should be terminated when the JVM exits.
     */
    public void setTerminateOnJvmExit(boolean flag)
    {
        this.terminateOnJvmExit = flag;
    }

    /**
     * Sets the working directory to execute the command from.
     * @param workingDirectory The working directory to execute the command from.
     */
    public void setWorkingDirectory(File workingDirectory)
    {
        this.workingDirectory = workingDirectory;
    }

    /**
     * Executes the process.
     * @return The exit code of the process.
     * @throws IOException if there was a I/O problem whilst executing the process.
     */
    public int execute() throws IOException
    {
        Runtime runtime = Runtime.getRuntime();

        int exitCode = -1;
        TerminateProcessThread terminateProcess = null;
        Process process = null;

        try
        {
            // Execute the command
            String[] command = (String[]) this.command.toArray(new String[this.command.size()]);

            if (this.workingDirectory != null)
            {
                process = runtime.exec(command, null, this.workingDirectory);
            }
            else
            {
                process = runtime.exec(command);
            }

            // Start the stream handler
            this.handler.setErrorStream(process.getErrorStream());
            this.handler.setInputStream(process.getInputStream());
            this.handler.setOutputStream(process.getOutputStream());

            this.handler.start();

            // Setup the thread to terminate the process if the JVM is shut down
            if (this.terminateOnJvmExit)
            {
                terminateProcess = new TerminateProcessThread(process);
                runtime.addShutdownHook(terminateProcess);
            }

            exitCode = process.waitFor();
        }
        catch (InterruptedException e)
        {
            process.destroy();
        }
        finally
        {
            // Stop the stream handler
            this.handler.stop();

            // Remove the terminate hook
            if (terminateProcess != null)
            {
                runtime.removeShutdownHook(terminateProcess);
            }

            // Close off the streams
            if (process != null)
            {
                closeStreams(process);
            }
        }

        return exitCode;
    }

    private static void closeStreams(Process process)
    {
        try
        {
            process.getErrorStream().close();
        }
        catch (IOException e)
        {
            // Ignore
        }

        try
        {
            process.getInputStream().close();
        }
        catch (IOException e)
        {
            // Ignore
        }

        try
        {
            process.getOutputStream().close();
        }
        catch (IOException e)
        {
            // Ignore
        }
    }

    private static class TerminateProcessThread extends Thread
    {
        private final Process process;

        private TerminateProcessThread(Process process)
        {
            this.process = process;
        }

        public void run()
        {
            this.process.destroy();
        }
    }
}
