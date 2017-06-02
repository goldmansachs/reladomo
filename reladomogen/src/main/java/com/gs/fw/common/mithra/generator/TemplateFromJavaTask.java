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
        ByteArrayOutputStream outStream = new ByteArrayOutputStream((int) inputFile.length());
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
                }
                line = reader.readLine();
            }
            outStream.close();
            copyIfChanged(outStream.toByteArray(), out);
            outStream = null;
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

    public void copyIfChanged(byte[] src, File outFile) throws IOException
    {
        boolean copyFile = false;
        if ((!outFile.exists()) || (outFile.length() != src.length))
        {
            copyFile = true;
        }
        else
        {
            byte[] outContent = readFile(outFile);
            for(int i=0;i<src.length;i++)
            {
                if (src[i] != outContent[i])
                {
                    copyFile = true;
                    break;
                }
            }
        }
        if (copyFile && outFile.exists() && !outFile.canWrite())
        {
            throw new IOException(outFile+" must be updated, but it is readonly.");
        }

        if (copyFile)
        {
            FileOutputStream fout = new FileOutputStream(outFile);
            fout.write(src);
            fout.close();
        }
    }

    private byte[] readFile(File file) throws IOException
    {
        int length = (int)file.length();
        FileInputStream fis = new FileInputStream(file);
        byte[] result = new byte[length];
        int pos = 0;
        while(pos < length)
        {
            pos += fis.read(result, pos, length - pos);
        }
        fis.close();
        return result;
    }
}
