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

package com.gs.fw.common.mithra.util.dbextractor;


import java.io.*;

import com.gs.fw.common.mithra.finder.*;

/**
 * Defines which text files extracted data will be written to, what file header to use and whether to overwrite or
 * append data. If data is appended to an existing file existing data is given precedence over new data and any
 * milestone overlaps are resolved and merged.
 *
 * @see com.gs.fw.common.mithra.util.dbextractor.MithraObjectGraphExtractor
 * @see com.gs.fw.common.mithra.util.dbextractor.MilestoneRectangle
 */
public interface OutputStrategy
{
    /**
     * Returns the output file to use for the given finder and source.
     *
     * @param relatedFinder - the finder instance of the extracted data
     * @param source - the source of the extracted data
     * @return the file to write extracted data to
     */
    File getOutputFile(RelatedFinder relatedFinder, Object source);

    /**
     * @return the output file header to add to new extract files
     */
    String getOutputFileHeader();

    /**
     * @return overwrite output file or append new data to existing
     */
    boolean overwriteOutputFile();
}
