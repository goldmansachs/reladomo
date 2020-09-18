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

import java.io.Serializable;
import java.util.regex.Pattern;

public class WildcardParser implements Serializable
{
    private static final long serialVersionUID = -2531503578551321454L;

    private String wildcardPattern;
    private String sqlExpression = null;
    private transient volatile Pattern regexPattern = null;

    public static final char SQL_ESCAPE = '=';
    public static final char REGEX_ESCAPE = '\\';

    private static final char WILDCARD_ESCAPE = '\'';
    private static final char[] SQL_META_CHARS = {'=', '%', '_'};

    private static final char WILDCARD_SUPPORTED_MULTIPLE_CHARS = '*';
    private static final char WILDCARD_SUPPORTED_SINGLE_CHAR = '?';
    private static final char SQL_SUPPORTED_EQUIVALENT_MULTIPLE_CHARS = '%';
    private static final char SQL_SUPPORTED_EQUIVALENT_SINGLE_CHAR = '_';
    private static final char[] REGEX_META_CHARS = {'\\', '.', '[', ']', '^', '$', '+',
                                  '{', '}', '|'};

    public WildcardParser(String wildcardPattern)
    {
        this.wildcardPattern = cleanPattern(wildcardPattern);
    }

    public static AnalyzedWildcardPattern getAnalyzedPattern(String pattern, AnalyzedWildcardPattern soFar)
    {
        if (soFar == null)
        {
            soFar = new AnalyzedWildcardPattern();
        }
        pattern = cleanPattern(pattern);
        boolean lastCharWasQuote = false;
        int wildCount = 0;
        int firstStarPosition = -1;
        int lastStarPosition = -1;
        for(int i=0;i<pattern.length();i++)
        {
            char c = pattern.charAt(i);
            if (c == WILDCARD_ESCAPE)
            {
                lastCharWasQuote = !lastCharWasQuote;
            }
            else if (c == WILDCARD_SUPPORTED_MULTIPLE_CHARS)
            {
                if (!lastCharWasQuote)
                {
                    wildCount++;
                    lastStarPosition = i;
                    if (firstStarPosition == -1)
                    {
                        firstStarPosition = i;
                    }
                }
                lastCharWasQuote = false;
            }
            else if (c == WILDCARD_SUPPORTED_SINGLE_CHAR)
            {
                if (!lastCharWasQuote)
                {
                    wildCount++;
                }
                lastCharWasQuote = false;
            }
            else
            {
                lastCharWasQuote = false;
            }
        }
        if (wildCount == 0)
        {
            soFar.addPlain(pattern);
        }
        else if (wildCount == 1 && lastStarPosition == pattern.length() - 1)
        {
            soFar.addSubstring(pattern.length() - 1, pattern.substring(0, pattern.length() - 1));
        }
        else if (wildCount == 1 && lastStarPosition == 0)
        {
            soFar.addEndsWith(pattern.substring(1, pattern.length()));
        }
        else if (wildCount == 2 && firstStarPosition == 0 && lastStarPosition == pattern.length() - 1)
        {
            soFar.addContains(pattern.substring(1, pattern.length() - 1));
        }
        else
        {
            soFar.addWildcard(pattern);
        }
        return soFar;
    }

    private static String cleanPattern(String pattern)
    {
        StringBuilder builder = null;
        boolean lastCharWasQuote = false;
        boolean lastCharWasStar = false;
        for(int i=0;i<pattern.length();i++)
        {
            char c = pattern.charAt(i);
            if (c == WILDCARD_ESCAPE)
            {
                lastCharWasQuote = !lastCharWasQuote;
                lastCharWasStar = false;
                if (builder != null) builder.append(c);
            }
            else if (c == WILDCARD_SUPPORTED_MULTIPLE_CHARS)
            {
                if (!lastCharWasQuote)
                {
                    if (lastCharWasStar)
                    {
                        if (builder == null)
                        {
                            builder = new StringBuilder(pattern.length() - 1);
                            builder.append(pattern.substring(0, i));
                        }
                    }
                    else
                    {
                        if (builder != null) builder.append(c);
                    }
                    lastCharWasStar = true;
                }
                else
                {
                    if (builder != null) builder.append(c);
                }
                lastCharWasQuote = false;
            }
            else
            {
                lastCharWasQuote = false;
                lastCharWasStar = false;
                if (builder != null) builder.append(c);
            }
        }
        return (builder == null) ? pattern : builder.toString();
    }

    public boolean matches(String candidate)
    {
        if (this.regexPattern == null)
        {
            this.generateRegexPattern();
        }
        return regexPattern.matcher(candidate).matches();
    }

    public String getSqlLikeExpression()
    {
        if (sqlExpression == null)
        {
            this.sqlExpression = this.generateSqlExpressionWithWildCard(this.wildcardPattern, SQL_META_CHARS);
        }
        return sqlExpression;
    }

    public String getSqlLikeExpression(char[] sqlMetaChars)
    {
        return this.generateSqlExpressionWithWildCard(this.wildcardPattern, sqlMetaChars);
    }

    public static String generateSqlExpressionWithWildCard(String wildcardPattern, char[] sqlMetaChars)
    {
        /* Escape SQL meta-characters  */

        boolean inEscapeSequence = false;
        char last = '!';
        String s = wildcardPattern;

        for (int i = 0; i < s.length(); i++)
        {
            char c = s.charAt(i);

            if ((c == WILDCARD_SUPPORTED_MULTIPLE_CHARS) && !inEscapeSequence)
            {
                s = s.substring(0, i) + SQL_SUPPORTED_EQUIVALENT_MULTIPLE_CHARS +
                        s.substring(i + 1, s.length());
            }

            if ((c == WILDCARD_SUPPORTED_SINGLE_CHAR) && !inEscapeSequence)
            {
                s = s.substring(0, i) + SQL_SUPPORTED_EQUIVALENT_SINGLE_CHAR +
                        s.substring(i + 1, s.length());
            }

            for (int j = 0; j < sqlMetaChars.length; j++)
            {
                if (c == sqlMetaChars[j])
                {
                    s = s.substring(0, i) + SQL_ESCAPE + sqlMetaChars[j] +
                            s.substring(i + 1, s.length());
                    i++;
                }
            }

            if (inEscapeSequence)
            {
                inEscapeSequence = false;
                last = c;
                continue;
            }

            if ((c == WILDCARD_ESCAPE) && (last != WILDCARD_ESCAPE))
            {
                inEscapeSequence = true;
                s = s.substring(0, i) + s.substring(i + 1, s.length());
                i--;
            }

            last = c;
        }
        return s;
    }

    public static String escapeLikeMetaChars(String pattern, char[] sqlMetaChars)
    {
        boolean inEscapeSequence = false;
        String s = pattern;

        for (int i = 0; i < s.length(); i++)
        {
            char c = s.charAt(i);

            for (int j = 0; j < sqlMetaChars.length; j++)
            {
                if (c == sqlMetaChars[j])
                {
                    s = s.substring(0, i) + SQL_ESCAPE + sqlMetaChars[j] +
                            s.substring(i + 1, s.length());
                    i++;
                }
            }
        }
        return s;
    }

    public static String escapeLikeMetaCharsForIq(String pattern)
    {
        boolean inEscapeSequence = false;
        String s = pattern;

        for (int i = 0; i < s.length(); i++)
        {
            char c = s.charAt(i);

            switch (c)
            {
                case '%':
                case '_':
                case '=':
                    s = s.substring(0,i) + '[' + c + ']' + s.substring(i + 1, s.length());
                    i += 2;
                    break;
                case '[':
                    s = s.substring(0,i) + '=' + s.substring(i, s.length());
                    i++;
                    break;
            }
        }
        return s;
    }

    private void generateRegexPattern()
    {
        /* Check one bad escape case */
        String glob = wildcardPattern;
        if (glob.equals("'"))
        {
            glob = "''";
        }

        /* Escape Java meta-characters and build regular expression from glob */
        boolean inEscapeSequence = false;
        char last = '!';

        for (int i = 0; i < glob.length(); i++)
        {
            char c = glob.charAt(i);

            if ((c == WILDCARD_SUPPORTED_MULTIPLE_CHARS) && !inEscapeSequence)
            {
                glob = glob.substring(0, i) + '.' + WILDCARD_SUPPORTED_MULTIPLE_CHARS +
                        glob.substring(i + 1, glob.length());
                i++;
            }
            else if ((c == WILDCARD_SUPPORTED_MULTIPLE_CHARS) && inEscapeSequence)
            {
                glob = glob.substring(0, i) + REGEX_ESCAPE + WILDCARD_SUPPORTED_MULTIPLE_CHARS +
                        glob.substring(i + 1, glob.length());
                i++;
            }

            if ((c == WILDCARD_SUPPORTED_SINGLE_CHAR) && !inEscapeSequence)
            {
                glob = glob.substring(0, i) + '.' +
                        glob.substring(i + 1, glob.length());
            }
            else if ((c == WILDCARD_SUPPORTED_SINGLE_CHAR) && inEscapeSequence)
            {
                glob = glob.substring(0, i) + REGEX_ESCAPE + WILDCARD_SUPPORTED_SINGLE_CHAR +
                        glob.substring(i + 1, glob.length());
                i++;
            }

            if (inEscapeSequence)
            {
                inEscapeSequence = false;
                last = c;
                continue;
            }

            for (int j = 0; j < REGEX_META_CHARS.length; j++)
            {
                if (c == REGEX_META_CHARS[j])
                {
                    glob = glob.substring(0, i) + REGEX_ESCAPE + REGEX_META_CHARS[j] +
                            glob.substring(i + 1, glob.length());
                    i++;
                }
            }

            if ((c == WILDCARD_ESCAPE) && (last != WILDCARD_ESCAPE))
            {
                inEscapeSequence = true;
                glob = glob.substring(0, i) + glob.substring(i + 1, glob.length());
                i--;
            }

            last = c;
        }

        /* Check if

        /* Ensure that test matches glob from beginning to end */
        glob = "^" + glob + "$";

        this.regexPattern = Pattern.compile(glob);
    }

    public String getSqlLikeExpressionForIq()
    {
        return this.generateSqlExpressionWithWildCardForIq(this.wildcardPattern);
    }

    public static String generateSqlExpressionWithWildCardForIq(String wildcardPattern)
    {
        /* Escape SQL meta-characters  */

        boolean inEscapeSequence = false;
        char last = '!';
        String s = wildcardPattern;

        for (int i = 0; i < s.length(); i++)
        {
            char c = s.charAt(i);

            if ((c == WILDCARD_SUPPORTED_MULTIPLE_CHARS) && !inEscapeSequence)
            {
                s = s.substring(0, i) + SQL_SUPPORTED_EQUIVALENT_MULTIPLE_CHARS +
                        s.substring(i + 1, s.length());
            }

            if ((c == WILDCARD_SUPPORTED_SINGLE_CHAR) && !inEscapeSequence)
            {
                s = s.substring(0, i) + SQL_SUPPORTED_EQUIVALENT_SINGLE_CHAR +
                        s.substring(i + 1, s.length());
            }

            switch (c)
            {
                case '%':
                case '_':
                case '=':
                    s = s.substring(0,i) + '[' + c + ']' + s.substring(i + 1, s.length());
                    i+=2;
                    break;
                case '[':
                    s = s.substring(0,i) + '=' + s.substring(i, s.length());
                    i++;
                    break;
            }

            if (inEscapeSequence)
            {
                inEscapeSequence = false;
                last = c;
                continue;
            }

            if ((c == WILDCARD_ESCAPE) && (last != WILDCARD_ESCAPE))
            {
                inEscapeSequence = true;
                s = s.substring(0, i) + s.substring(i + 1, s.length());
                i--;
            }

            last = c;
        }
        return s;
    }

}
