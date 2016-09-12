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

package com.gs.fw.common.mithra.generator;

import org.apache.tools.ant.Task;
import org.apache.tools.ant.BuildException;

import java.io.*;


public class TemplateFromJavaTask extends Task
{

    private String input;
    private String startDelimeter = "//beginTemplate";
    private String endDelimeter = "//endTemplate";
    private String output;

    public void setOutput(String output)
    {
        this.output = output;
    }

    public void setEndDelimeter(String endDelimeter)
    {
        this.endDelimeter = endDelimeter;
    }

    public void setInput(String input)
    {
        this.input = input;
    }

    public void setStartDelimeter(String startDelimeter)
    {
        this.startDelimeter = startDelimeter;
    }

    public void execute() throws BuildException
    {
        File inputFile = new File(this.input);
        if (!inputFile.exists())
        {
            throw new BuildException("the input "+this.input +" does not exist");
        }
        FileOutputStream outStream = null;
        LineNumberReader reader = null;
        try
        {
            reader = new LineNumberReader(new FileReader(inputFile));
            String line = reader.readLine();
            File out = null;
            boolean writing = false;
            while(line != null)
            {
                if (writing)
                {
                    if (line.startsWith(endDelimeter))
                    {
                        writing = false;
                        outStream.close();
                    }
                    else
                    {
                        outStream.write(line.getBytes());
                        outStream.write('\n');
                    }
                }
                else if (line.startsWith(startDelimeter))
                {
                    String templateSection = line.substring(startDelimeter.length()+1).trim();
                    writing = true;
                    out = new File(output+"."+templateSection);
                    outStream = new FileOutputStream(out);
                }
                line = reader.readLine();
            }
        }
        catch(IOException e)
        {
            throw new BuildException("could not write template", e);
        }
        finally
        {
            try
            {
                if (outStream != null) outStream.close();
                if (reader!= null) reader.close();
            }
            catch (IOException e)
            {
                // ignore
            }
        }
    }
}
