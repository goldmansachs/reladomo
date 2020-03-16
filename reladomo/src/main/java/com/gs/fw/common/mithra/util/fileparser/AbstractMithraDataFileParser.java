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

package com.gs.fw.common.mithra.util.fileparser;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.net.URL;
import java.nio.charset.Charset;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.List;



public abstract class AbstractMithraDataFileParser
{
    static private Logger logger = LoggerFactory.getLogger(AbstractMithraDataFileParser.class.getName());
    protected static final String CLASS_IDENTIFIER = "class";

    private URL streamLocation;
    private InputStream inputStream;
    private boolean inputStreamFromFilesystemOnly;
    private String filename;
    private File file;
    private List<MithraParsedData> results;
    private ParserState beginningOfLineState;
    private ParserState classReaderState;
    private DataReaderState dataReaderState;
    private AttributeReaderState attributeReaderState;
    private MithraParsedData currentParsedData;
    private Charset charset;

    public AbstractMithraDataFileParser(String filenameFromClasspathOrFileSystem)
    {
        this.inputStreamFromFilesystemOnly = false;
        this.file = null;
        this.filename = filenameFromClasspathOrFileSystem;
        this.initStates();
    }

    public AbstractMithraDataFileParser(File file)
    {
        this.inputStreamFromFilesystemOnly = true;
        this.file = file;
        this.filename = null;
        this.initStates();
    }

    public AbstractMithraDataFileParser(URL streamLocation, InputStream is)
    {
        this.streamLocation = streamLocation;
        this.inputStream = is;
        this.initStates();
    }

    public void setCharset(Charset charset)
    {
        this.charset = charset;
    }

    private void initStates()
    {
        this.classReaderState = this.createClassReaderState();
        this.dataReaderState = this.createDataReaderState();
        this.beginningOfLineState = this.createBeginningOfLineState();
        this.attributeReaderState = this.createAttributeReaderState();
    }

    protected abstract ParserState createBeginningOfLineState();
    protected abstract ParserState createClassReaderState();
    protected abstract DataReaderState createDataReaderState();
    protected abstract AttributeReaderState createAttributeReaderState();

    protected ParserState getBeginningOfLineState()
    {
        return beginningOfLineState;
    }

    protected ParserState getClassReaderState()
    {
        return classReaderState;
    }

    protected DataReaderState getDataReaderState()
    {
        return dataReaderState;
    }

    protected AttributeReaderState getAttributeReaderState()
    {
        return attributeReaderState;
    }

    protected MithraParsedData getCurrentParsedData()
    {
        return currentParsedData;
    }

    protected void setCurrentParsedData(MithraParsedData currentParsedData)
    {
        this.currentParsedData = currentParsedData;
    }

    private InputStream getInputStreamForFile() throws IOException
    {
        if (this.inputStream != null)
        {
            return this.inputStream;
        }
        InputStream is;
        if (this.inputStreamFromFilesystemOnly)
        {
            is = this.getInputStreamFromFile();
        }
        else
        {
            is = this.getInputStreamFromFilename();
        }

        return is;
    }

    protected InputStream getInputStreamFromFilename() throws IOException
    {
        InputStream is = this.getClass().getClassLoader().getResourceAsStream(this.getFilename());

        if (is == null)
        {
            this.file = new File(this.filename);
            is = getInputStreamFromFile();
        }

        return is;
    }

    protected InputStream getInputStreamFromFile() throws IOException
    {
        InputStream is;

        try
        {
            is = new FileInputStream(this.getFile());
        }
        catch (FileNotFoundException e)
        {
            String inLocation = "";
            try
            {
                inLocation = " in " + new File(".").getAbsolutePath();
            } catch (Exception e2)
            {
                //ignore
            }

            FileNotFoundException e2 = new FileNotFoundException("could not find file " + this.getFile() + inLocation);
            e2.initCause(e);
            throw e2;
        }

        return is;
    }

    protected void parse()
    {
        if (this.results == null)
        {
            this.results = new ArrayList<MithraParsedData>();

            InputStream is = null;
            try
            {
                try
                {
                    is = this.getInputStreamForFile();
                }
                catch (IOException e)
                {
                    throw new RuntimeException("could not get file input stream", e);
                }

                InputStreamReader streamReader;
                if (this.charset != null)
                {
                    streamReader = new InputStreamReader(is, charset);
                }
                else
                {
                    streamReader = new InputStreamReader(is);
                }
                Reader reader = new BufferedReader(streamReader);
                StreamTokenizer st = new StreamTokenizer(reader);
                st.parseNumbers();
                st.eolIsSignificant(true);
                st.wordChars('_', '_');
    
                // These calls caused comments to be discarded
                st.slashSlashComments(true);
                st.slashStarComments(true);
    
                // Parse the file
                ParserState currentState = this.getBeginningOfLineState();
                while (currentState != null)
                {
                    try
                    {
                        currentState = currentState.parse(st);
                    }
                    catch (IOException e)
                    {
                        throwParseException(st, e);
                    }
                    catch (ParseException e)
                    {
                        throwParseException(st, e);
                    }
                }
            }
            finally 
            {
                if (is != null)
                {
                    try
                    {
                        is.close();
                    }
                    catch (IOException e)
                    {
                        logger.error("Could not close input stream", e);
                    }
                }
            }
        }
    }

    private void throwParseException(StreamTokenizer st, Exception e)
    {
        String fname = this.filename;
        if (!this.inputStreamFromFilesystemOnly)
        {
            if(this.getFile() != null) fname = this.getFile().getName();
        }
        if(this.streamLocation != null)
        {
            fname = this.streamLocation.toString();
        }
        ParseException parseException = new ParseException("Unexpected exception while parsing line " + st.lineno() + " of " + fname, st.lineno());
        parseException.initCause(e);
        throw new RuntimeException(parseException);
    }

    public List<MithraParsedData> getResults()
    {
        parse();
        return this.results;
    }

    protected void addNewMithraParsedData()
    {
        this.currentParsedData = new MithraParsedData();
        this.results.add(this.currentParsedData);
    }

    public static Logger getLogger()
    {
        return logger;
    }

    public File getFile()
    {
        return file;
    }

    public String getFilename()
    {
        return filename;
    }
}
