<?xml version='1.0'?>
<!--
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
  -->

<xsl:stylesheet xmlns:xsl="http://www.w3.org/1999/XSL/Transform"
    xmlns:fo="http://www.w3.org/1999/XSL/Format"
    xmlns:xslthl="http://xslthl.sf.net"
    version='1.0'>

    <xsl:import href="../../../../../target/docbook-lib/docbook/highlighting/common.xsl" />

    <xsl:template match='xslthl:keyword' mode="xslthl">
        <fo:inline font-weight="bold" color="#000080">
            <xsl:apply-templates mode="xslthl" />
        </fo:inline>
    </xsl:template>

    <xsl:template match='xslthl:string' mode="xslthl">
        <fo:inline font-weight="bold" color="green">
            <xsl:apply-templates mode="xslthl" />
        </fo:inline>
    </xsl:template>

    <xsl:template match='xslthl:comment' mode="xslthl">
        <fo:inline color="gray">
            <xsl:apply-templates mode="xslthl" />
        </fo:inline>
    </xsl:template>

    <xsl:template match='xslthl:multiline-comment' mode="xslthl">
        <fo:inline color="gray">
            <xsl:apply-templates mode="xslthl" />
        </fo:inline>
    </xsl:template>

    <xsl:template match='xslthl:tag' mode="xslthl">
        <fo:inline color="navy">
            <xsl:apply-templates mode="xslthl" />
        </fo:inline>
    </xsl:template>

    <xsl:template match='xslthl:attribute' mode="xslthl">
        <fo:inline color="blue">
            <xsl:apply-templates mode="xslthl" />
        </fo:inline>
    </xsl:template>

    <xsl:template match='xslthl:value' mode="xslthl">
        <fo:inline color="green">
            <xsl:apply-templates mode="xslthl" />
        </fo:inline>
    </xsl:template>

    <xsl:template match='xslthl:html' mode="xslthl">
        <fo:inline color="navy" font-weight="bold">
            <xsl:apply-templates mode="xslthl" />
        </fo:inline>
    </xsl:template>

    <xsl:template match='xslthl:xslt' mode="xslthl">
        <fo:inline color="#0066FF">
            <xsl:apply-templates mode="xslthl" />
        </fo:inline>
    </xsl:template>

    <xsl:template match='xslthl:section' mode="xslthl">
        <fo:inline font-weight="bold">
            <xsl:apply-templates mode="xslthl" />
        </fo:inline>
    </xsl:template>

    <xsl:template match='xslthl:number' mode="xslthl">
        <fo:inline color="blue">
            <xsl:apply-templates mode="xslthl" />
        </fo:inline>
    </xsl:template>

    <xsl:template match='xslthl:annotation' mode="xslthl">
        <fo:inline color="gray">
            <xsl:apply-templates mode="xslthl" />
        </fo:inline>
    </xsl:template>

    <xsl:template match='xslthl:directive' mode="xslthl">
        <xsl:apply-templates mode="xslthl" />
    </xsl:template>

    <xsl:template match='xslthl:doccomment' mode="xslthl">
        <fo:inline font-weight="bold" color="#4D4DFF">
            <xsl:apply-templates mode="xslthl" />
        </fo:inline>
    </xsl:template>

</xsl:stylesheet>

