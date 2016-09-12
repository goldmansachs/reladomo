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

import com.gs.fw.common.mithra.generator.util.StringUtility;

import java.util.List;
import java.util.ArrayList;
import java.io.*;


public class GenerationLog
{

    private String md5;
    private String xmlCrc;

    public GenerationLog(String md5, String xmlCrc)
    {
        this.md5 = md5;
        this.xmlCrc = xmlCrc;
    }

    public String getMd5()
    {
        return md5;
    }

    public String getXmlCrc()
    {
        return xmlCrc;
    }

    public void writeLog(String generatedDir, String classListName) throws IOException
    {
        String logNameFromClassListName = getLogNameFromClassListName(generatedDir, classListName);
        System.out.println("writing log to "+logNameFromClassListName);
        File file = new File(logNameFromClassListName);
        FileWriter writer = new FileWriter(file);
        writer.write(this.md5+"\n");
        writer.write(this.xmlCrc+"\n");
        writer.close();
    }

    private static String getLogNameFromClassListName(String generatedDir, String classListName)
    {
        String cleanedGeneratedDir = getCleanedDir(generatedDir);

        String cleanedClassListName = getCleanedDir(classListName);

        int maxSearchLength = cleanedGeneratedDir.length();
        if (cleanedClassListName.length() < maxSearchLength) maxSearchLength = cleanedClassListName.length();

        int matchedCharIndex = 0;
        for(int i=0;i<maxSearchLength;i++)
        {
            if (Character.toUpperCase(cleanedClassListName.charAt(i)) == Character.toUpperCase(cleanedGeneratedDir.charAt(i)))
            {
                matchedCharIndex++;
            }
            else
            {
                break;
            }
        }

        return generatedDir+File.separator+cleanedClassListName.substring(matchedCharIndex, cleanedClassListName.length())+".log";
    }

    private static String getCleanedDir(String generatedDir)
    {
        String cleanedGeneratedDir = StringUtility.replaceStr(generatedDir, "/", ".");
        cleanedGeneratedDir = StringUtility.replaceStr(cleanedGeneratedDir, "\\", ".");
        cleanedGeneratedDir = StringUtility.replaceStr(cleanedGeneratedDir, File.separator, ".");
        cleanedGeneratedDir = StringUtility.replaceStr(cleanedGeneratedDir, ":", ".");
        return cleanedGeneratedDir;
    }

    public static GenerationLog readOldLog(String generatedDir, String fileNameForClassList) throws IOException
    {
        GenerationLog result = new GenerationLog("xxx", "00");
        File file = new File(getLogNameFromClassListName(generatedDir, fileNameForClassList));
        if (file.exists())
        {
            FileInputStream fis = new FileInputStream(file);
            try
            {
                LineNumberReader reader = new LineNumberReader(new InputStreamReader(fis));
                String md5 = reader.readLine();
                String xmlCrc = reader.readLine();
                if (md5 != null && xmlCrc != null)
                {
                    result = new GenerationLog(md5, xmlCrc);
                }
            }
            finally
            {
                fis.close();
            }
        }
        return result;
    }

    public boolean isSame(GenerationLog oldLog)
    {
        return this.getMd5().equals(oldLog.getMd5()) && this.getXmlCrc().equals(oldLog.getXmlCrc());
    }
}
