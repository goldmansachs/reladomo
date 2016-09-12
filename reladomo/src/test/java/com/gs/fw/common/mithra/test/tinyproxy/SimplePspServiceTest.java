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

package com.gs.fw.common.mithra.test.tinyproxy;

import java.io.IOException;
import java.net.MalformedURLException;

import com.gs.fw.common.mithra.test.util.tinyproxy.FastServletProxyFactory;
import com.gs.fw.common.mithra.test.util.tinyproxy.PspRuntimeException;
import org.junit.Assert;

public class SimplePspServiceTest
        extends PspTestCase
{
    public void testEcho() throws MalformedURLException, InterruptedException
    {
        Echo echo = this.buildEchoProxy();

        Assert.assertEquals("hello", echo.echo("hello"));
        for (int i = 0; i < 100; i++)
        {
            Thread.sleep(100L);
            if (this.servlet.getThankYous() > 0)
            {
                break;
            }
        }
        Assert.assertTrue(this.servlet.getThankYous() > 0);
    }

    public void testException() throws MalformedURLException
    {
        Echo echo = this.buildEchoProxy();

        try
        {
            echo.throwExpectedException();
            Assert.fail("should not get here");
        }
        catch (FakeException e)
        {
            // expected
        }
        Assert.assertEquals("hello", echo.echo("hello"));
    }

    public void testUnexpectedException() throws MalformedURLException
    {
        Echo echo = this.buildEchoProxy();
        try
        {
            echo.throwUnexpectedException();
            Assert.fail("should not get here");
        }
        catch (RuntimeException e)
        {
            // expected
        }
        Assert.assertEquals("hello", echo.echo("hello"));
    }

    public void testResendRequest() throws MalformedURLException
    {
        Echo echo = this.buildEchoProxy();

        StringBuilder largeBuffer = new StringBuilder(5000);
        for (int i = 0; i < 5000; i++)
        {
            largeBuffer.append(i);
        }
        Assert.assertTrue(largeBuffer.length() > 5000);
        String largeString = largeBuffer.toString();
        Assert.assertEquals(largeString, echo.echoWithException(largeString).getContents());
    }

    public void testUnserializableObject() throws MalformedURLException
    {
        Echo echo = this.buildEchoProxy();

        for (int i = 0; i < 50; i++)
        {
            try
            {
                echo.testUnserializableObject(new Object());
                Assert.fail("should not get here");
            }
            catch (PspRuntimeException e)
            {
            }
        }
        Assert.assertEquals("hello", echo.echo("hello"));
    }

    public void testPing() throws IOException
    {
        FastServletProxyFactory fspf = new FastServletProxyFactory();
        Assert.assertTrue(fspf.isServiceAvailable(this.getPspUrl()));
    }
}
