<?xml version="1.0"?>
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
    xmlns:xslthl="http://xslthl.sf.net" version="1.0">

    <xsl:import href="../../../../target/docbook-lib/docbook/highlighting/common.xsl" />

    <xsl:template match="xslthl:keyword" mode="xslthl">
        <strong class="hl-keyword">
            <span style="color: #000080">
                <xsl:apply-templates mode="xslthl" />
            </span>
        </strong>
    </xsl:template>
    <xsl:template match="xslthl:string" mode="xslthl">
        <strong class="hl-string">
            <span style="color: green">
                <xsl:apply-templates mode="xslthl" />
            </span>
        </strong>
    </xsl:template>
    <xsl:template match="xslthl:comment" mode="xslthl">
        <em class="hl-comment" style="color: gray">
            <xsl:apply-templates mode="xslthl" />
        </em>
    </xsl:template>
    <xsl:template match="xslthl:multiline-comment" mode="xslthl">
        <em class="hl-comment" style="color: gray">
            <xsl:apply-templates mode="xslthl" />
        </em>
    </xsl:template>
    <xsl:template match="xslthl:directive" mode="xslthl">
        <span class="hl-directive" style="color: maroon">
            <xsl:apply-templates mode="xslthl" />
        </span>
    </xsl:template>
    <xsl:template match="xslthl:tag" mode="xslthl">
        <strong class="hl-tag" style="color: navy">
            <xsl:apply-templates mode="xslthl" />
        </strong>
    </xsl:template>
    <xsl:template match="xslthl:attribute" mode="xslthl">
        <span class="hl-attribute" style="color: blue">
            <xsl:apply-templates mode="xslthl" />
        </span>
    </xsl:template>
    <xsl:template match="xslthl:value" mode="xslthl">
        <span class="hl-value" style="color: green">
            <xsl:apply-templates mode="xslthl" />
        </span>
    </xsl:template>
    <xsl:template match='xslthl:html' mode="xslthl">
        <span class="hl-html" style="color: navy; font-weight: bold">
            <xsl:apply-templates mode="xslthl" />
        </span>
    </xsl:template>
    <xsl:template match="xslthl:xslt" mode="xslthl">
        <strong style="color: #0066FF">
            <xsl:apply-templates mode="xslthl" />
        </strong>
    </xsl:template>
    <!-- Not emitted since XSLTHL 2.0 -->
    <xsl:template match="xslthl:section" mode="xslthl">
        <strong>
            <xsl:apply-templates mode="xslthl" />
        </strong>
    </xsl:template>
    <xsl:template match="xslthl:number" mode="xslthl">
        <span class="hl-number" style="color:blue">
            <xsl:apply-templates mode="xslthl" />
        </span>
    </xsl:template>
    <xsl:template match="xslthl:annotation" mode="xslthl">
        <em>
            <span class="hl-annotation" style="color: gray">
                <xsl:apply-templates mode="xslthl" />
            </span>
        </em>
    </xsl:template>

    <xsl:template match="xslthl:doccomment" mode="xslthl">
        <strong class="hl-tag" style="color: #4D4DFF">
            <xsl:apply-templates mode="xslthl" />
        </strong>
    </xsl:template>
</xsl:stylesheet>
