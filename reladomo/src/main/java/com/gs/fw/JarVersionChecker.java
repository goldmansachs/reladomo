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

package com.gs.fw;

import java.io.*;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.List;
import java.util.zip.CRC32;


public class JarVersionChecker
{
    private String applicationName;
    private String version;
    private boolean processed;
    private boolean isVersionInfoPresent;
    private String reason;
    private boolean isVersionInfoConsistent;
    private List<FileVersionMismatch> mismatches = new ArrayList<FileVersionMismatch>();
    private CRC32 crc32 = new CRC32();
    private CRC32 fullFileCrc32 = new CRC32();
    private byte[] buf = new byte[2048];
    private String fullFileCrc;


    public JarVersionChecker(String applicationName)
    {
        this.applicationName = applicationName;
    }

    public boolean isVersionInfoPresent()
    {
        process();
        return this.isVersionInfoPresent;
    }

    private synchronized void process()
    {
        if (processed) return;
        processed = true;
        LineNumberReader reader = null;
        try
        {
            InputStream stream = this.getClass().getResourceAsStream("/META-INF/"+this.applicationName+".crcver");
            if (stream != null)
            {
                reader = new LineNumberReader(new InputStreamReader(stream, "UTF8"));
                parseApplicationName(reader);
                parseVersion(reader);
                fullFileCrc32.update(applicationName.getBytes("UTF8"));
                fullFileCrc32.update(version.getBytes("UTF8"));
                String leftOver = parseFiles(reader);
                parseFullFileCrc(reader, leftOver);
            }
        }
        catch (IOException e)
        {
            reason = "Error reading version information: "+e.getClass().getName()+": "+e.getMessage();
        }
        catch (ParseException e)
        {
            this.reason = e.getMessage() + " (near or on line "+e.getErrorOffset()+ ')';
        }
        finally
        {
            try
            {
                if (reader != null) reader.close();
            }
            catch (IOException e)
            {
                //ignore
            }
        }
    }

    private void parseFullFileCrc(LineNumberReader reader, String leftOver) throws ParseException
    {
        if (leftOver == null)
        {
            throw new ParseException("Could not find :crc: line", reader.getLineNumber());
        }
        if (!leftOver.startsWith(":crc:"))
        {
            throw new ParseException("Expected a line with :crc:, but got \n"+leftOver, reader.getLineNumber());
        }
        String expectedCrc = Long.toHexString(fullFileCrc32.getValue());
        this.fullFileCrc = expectedCrc;
        String fileCrc = leftOver.substring(":crc: ".length()).trim();
        this.isVersionInfoPresent = true;
        if (expectedCrc.equals(fileCrc))
        {
            this.isVersionInfoConsistent = true;
        }
        else
        {
            reason = "Internal file consistency does not match. Expecting "+expectedCrc+" but got "+fileCrc;
        }
    }

    private String parseFiles(LineNumberReader reader) throws IOException, ParseException
    {
        String line = skipBlanks(reader);
        String pkg = "";
        while(line != null && !line.startsWith(":crc"))
        {
            if (line.startsWith("/"))
            {
                pkg = line.trim().substring(1);
            }
            else
            {
                verifyFile(line, pkg, reader.getLineNumber());
            }
            line = skipBlanks(reader);
        }
        return line;
    }

    private void verifyFile(String line, String pkg, int lineNumber) throws ParseException, IOException
    {
        int colon = line.indexOf(':');
        if (colon <= 0)
        {
            throw new ParseException("Could not parse file name and crc from line \n"+line, lineNumber);
        }
        String filePath = pkg+line.substring(0, colon);
        fullFileCrc32.update(filePath.getBytes("UTF8"));
        String expectedCrc = line.substring(colon+1).trim();
        fullFileCrc32.update(expectedCrc.getBytes("UTF8"));
        InputStream stream = this.getClass().getResourceAsStream('/' +filePath);
        if (stream == null)
        {
            mismatches.add(new FileVersionMismatch(filePath, expectedCrc, null));
        }
        else
        {
            try
            {
                String crc = computeSingleFileCrc(stream);
                if (!crc.equals(expectedCrc))
                {
                    mismatches.add(new FileVersionMismatch(filePath, expectedCrc, crc));
                }
            }
            catch (IOException e)
            {
                mismatches.add(new FileVersionMismatch(filePath, expectedCrc, null));
            }
            finally
            {
                stream.close();
            }
        }
    }

    private String computeSingleFileCrc(InputStream stream) throws IOException
    {
        crc32.reset();
        int read;
        while((read = stream.read(buf)) >= 0)
        {
            crc32.update(buf, 0, read);
        }
        return Long.toHexString(crc32.getValue());
    }

    private void parseVersion(LineNumberReader reader) throws IOException, ParseException
    {
        this.version = parseValue(reader, "version: ");
    }

    private void parseApplicationName(LineNumberReader reader) throws IOException, ParseException
    {
        String name = parseValue(reader, "name: ");
        if (!name.equals(this.applicationName))
        {
            this.isVersionInfoPresent = false;
            throw new ParseException("Was expecting application name '"+applicationName+"' but got '"+name+"'.", reader.getLineNumber());
        }
    }

    private String parseValue(LineNumberReader reader, String key)
            throws IOException, ParseException
    {
        String line = skipBlanks(reader);
        if (line == null)
        {
            this.isVersionInfoPresent = false;
            throw new ParseException("Could not find line starting with '"+key+ '\'', 0);
        }
        if (!line.startsWith(key))
        {
            this.isVersionInfoPresent = false;
            throw new ParseException("Could not find line starting with '"+key+"'. Instead the line at row "+reader.getLineNumber()+" read: \n"+line, reader.getLineNumber());
        }
        return line.substring(key.length()).trim();
    }

    private String skipBlanks(LineNumberReader reader)
            throws IOException
    {
        String line = reader.readLine();
        while(line != null && (line.startsWith("#") || line.trim().length() == 0))
        {
            line = reader.readLine();
        }
        return line;
    }

    public boolean isVersionInfoConsistent()
    {
        process();
        return this.isVersionInfoConsistent;
    }

    public String getVersion()
    {
        process();
        return this.version;
    }

    public List<FileVersionMismatch> getMismatches()
    {
        process();
        return this.mismatches;
    }

    public static void main(String[] args)
    {
        if (args.length < 1 || args.length > 1)
        {
            System.err.println("Usage: JarVersionChecker <appName>");
            System.exit(1);
        }
        JarVersionChecker checker = new JarVersionChecker(args[0]);
        if (!checker.isVersionInfoPresent())
        {
            System.out.println("ERROR: The internal version file for application "+args[0]+" could not be found in META-INF/"+args[0]+".crcver\n"+checker.getReason());
            System.exit(1);
        }
        if (!checker.isVersionInfoConsistent())
        {
            System.out.println("ERROR: The internal version file is not consistent. The following results cannot be trusted.\n"+checker.getReason());
        }
        System.out.println("Name: "+args[0]+" Version: "+checker.getVersion());
        List<FileVersionMismatch> mismatchList = checker.getMismatches();
        if (!mismatchList.isEmpty())
        {
            System.out.println(mismatchList.size()+" mismatches (file: expected/actual CRC):");
            for(int i=0;i<mismatchList.size();i++)
            {
                FileVersionMismatch mismatch = mismatchList.get(i);
                System.out.println(mismatch.getFilename()+": "+mismatch.getExpectedCrc()+ '/' +mismatch.getActualCrc());
            }
        }
        else if (checker.isVersionInfoConsistent())
        {
            System.out.println("is internally consistent. External checksum: "+checker.getFullFileCrc());
        }
    }

    public String getReason()
    {
        process();
        return reason;
    }

    public String getFullFileCrc()
    {
        process();
        return fullFileCrc;
    }

    public static class FileVersionMismatch
    {
        private String filename;
        private String expectedCrc;
        private String actualCrc;

        public FileVersionMismatch(String filename, String expectedCrc, String actualCrc)
        {
            this.filename = filename;
            this.expectedCrc = expectedCrc;
            this.actualCrc = actualCrc;
        }

        public String getFilename()
        {
            return filename;
        }

        public String getExpectedCrc()
        {
            return expectedCrc;
        }

        public String getActualCrc()
        {
            return actualCrc;
        }
    }
}
