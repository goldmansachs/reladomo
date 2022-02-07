# <repo>, <groupId>, <artifactId>, <version>, <extension>, <destDir>, <checksum>
# central,org.apache.ant,ant,1.9.6,jar,build/lib,80E2063B01BAB3C79C2D84E4ED5E73868394C85A
central,org.eclipse.collections,eclipse-collections-api,11.0.0,jar,lib/compile,1FA4325CDAD80CFEC8ADE827B8F8D00A54936E35
central,org.eclipse.collections,eclipse-collections,11.0.0,jar,lib/compile,FDEAF9CBD78FA80AB3F96B9DB9567538B20B1A6E
central,joda-time,joda-time,2.10.13,jar,lib/compile,86F338C18CEA2A89005556642E81707FF920DD38
central,org.apache.geronimo.specs,geronimo-jta_1.1_spec,1.1.1,jar,lib/compile,AABAB3165B8EA936B9360ABBF448459C0D04A5A4
central,org.slf4j,slf4j-api,1.7.35,jar,lib/compile,517F3A0687490B72D0E56D815E05608A541AF802

#drivers
central,org.postgresql,postgresql,9.3-1101-jdbc4,jar,lib/drivers,9DA59F12BADEA19B3B2884161F624BCF6750F985
central,org.mariadb.jdbc,mariadb-java-client,1.6.4,jar,lib/drivers,D78298105516737322552E71F3AB948C1E08F189

# test libs:
central,log4j,log4j,1.2.17,jar,lib/test,5AF35056B4D257E4B64B9E8069C0746E8B08629F
central,org.slf4j,slf4j-log4j12,1.7.21,jar,lib/test,7238B064D1ABA20DA2AC03217D700D91E02460FA
central,org.slf4j,jcl-over-slf4j,1.7.21,jar,lib/test,331B564A3A42F002A0004B039C1C430DA89062CD
central,org.apache.geronimo.specs,geronimo-jms_1.1_spec,1.1.1,jar,lib/test,C872B46C601D8DC03633288B81269F9E42762CEA
central,com.h2database,h2,2.1.210,jar,lib/test,A7395AE43062F9237EB441137B789C518C7D4C2F
central,org.mortbay.jetty,jetty,6.1.26,jar,lib/test,2F546E289FDDD5B1FAB1D4199FBB6E9EF43EE4B0
central,org.mortbay.jetty,jetty-util,6.1.26,jar,lib/test,E5642FE0399814E1687D55A3862AA5A3417226A9
central,javax.servlet,javax.servlet-api,3.0.1,jar,lib/test,6BF0EBB7EFD993E222FC1112377B5E92A13B38DD
central,junit,junit,4.11,jar,lib/test,4E031BB61DF09069AEB2BFFB4019E7A5034A4EE0
central,mockobjects,mockobjects-jdk1.4,0.09,jar,lib/test,3E91FB9024C6CB782D57DC28504EEFB936A7FDE5
central,mockobjects,mockobjects-core,0.09,jar,lib/test,2B3F525B29B03F420E4027083F25AE957D955D3A
central,org.apache.derby,derby,10.9.1.0,jar,lib/test,4538CF5564AB3C262EEC65C55FDB13965625589C
central,org.apache.derby,derbynet,10.9.1.0,jar,lib/test,6B40A22F530A83878752FEE1129556246F6ADBFD
central,org.apache.activemq,activemq-core,5.3.0,jar,lib/test,4C0581D16E94837938C7DC6A6879CAA0194DFAC8
central,org.apache.geronimo.specs,geronimo-j2ee-management_1.0_spec,1.1,jar,lib/test,057E32617E47E84FC7A26C7C9D3470B2D253AC34
#central,com.h2database,h2,1.3.169,jar,lib/test,

# test coverage libs:
central,org.jacoco,org.jacoco.core,0.7.9,jar,lib/coverage,66215826A684EB6866D4C14A5A4F9C344F1D1EEF
central,org.jacoco,org.jacoco.report,0.7.9,jar,lib/coverage,8A7F78FDF2A4E58762890D8E896A9298C2980C10
central,org.jacoco,org.jacoco.agent,0.7.9,jar,lib/coverage,4A936CAAB50B117A14D9CA3A725FC9B54D0CC3D1
central,org.jacoco,org.jacoco.ant,0.7.9,jar,lib/coverage,7CB39A4B38A32FFC8D0B5055B9B6C961ECFFA1B0
central,org.ow2.asm,asm-debug-all,5.2,jar,lib/coverage,3354E11E2B34215F06DAB629AB88E06ACA477C19

#serial libs:
central,com.google.code.gson,gson,2.8.9,jar,lib/serial,8A432C1D6825781E21A02DB2E2C33C5FDE2833B9

central,com.fasterxml.jackson.core,jackson-core,2.13.1,jar,lib/serial,51AE921A2ED1E06CA8876F12F32F265E83C0B2B8
central,com.fasterxml.jackson.core,jackson-databind,2.13.1,jar,lib/serial,698B2D2B15D9A1B7AAE025F1D9F576842285E7F6

central,javax.ws.rs,javax.ws.rs-api,2.0,jar,lib/serial,61F0983EB190954CCDEDE31E786A9E0BD9767C4A

central,com.fasterxml.jackson.core,jackson-annotations,2.13.1,jar,lib/serialtest,1CBCBE4623113E6AF92CCAA89884A345270F1A87

central,com.fasterxml.jackson.jaxrs,jackson-jaxrs-json-provider,2.13.1,jar,lib/serialtest,F305A0891E9C917FDD526F3708896B99CF89AF5C
central,com.fasterxml.jackson.jaxrs,jackson-jaxrs-base,2.13.1,jar,lib/serialtest,2C8AA6362A140F5DE4CD6292F9D92AE09DC03F34

central,org.glassfish.jersey.core,jersey-common,2.25,jar,lib/serialtest,B38E1A1AF6AD75DD9037A767764B0D41801F1C9F
central,org.glassfish.jersey.core,jersey-server,2.25,jar,lib/serialtest,0874CA16134872781B8C6A2ADFE3BB2AF80BC378
central,org.glassfish.jersey.core,jersey-client,2.25,jar,lib/serialtest,416A00562F25D7EFDBA015E5C21876D19163EDD0
central,org.glassfish.jersey.ext,jersey-entity-filtering,2.25,jar,lib/serialtest,52B5A204A79D22041F4B30C2670E95C2456CBCBD
central,org.glassfish.jersey.media,jersey-media-json-jackson,2.25,jar,lib/serialtest,2C99D40047A50FC43C5886545F7F2148C7E4A384
central,org.glassfish.jersey.media,jersey-media-jaxb,2.25,jar,lib/serialtest,09AA3EEBA90DCE24F04BF27CD1A4E0D378EC697F
central,org.glassfish.jersey.bundles.repackaged,jersey-guava,2.25,jar,lib/serialtest,4439BDDB870B210E40CA2E953813930BE424DC0C

central,org.glassfish.hk2,hk2-api,2.5.0-b30,jar,lib/serialtest,5C6688A6BAFCD2098BEF4CA45226D5355B816647
central,org.glassfish.hk2,hk2-utils,2.5.0-b30,jar,lib/serialtest,B17FC7D8082AC00E59CD96FDA9CFF21F24CC367C
central,org.glassfish.hk2,hk2-locator,2.5.0-b30,jar,lib/serialtest,82056CBBD258647BBC6B80DEBE4E6B7121C61BE9
central,org.glassfish.hk2.external,javax.inject,2.5.0-b30,jar,lib/serialtest,054B36144FD2FB684F9CEE73D96060BB82E4D363
central,org.glassfish.jersey.containers,jersey-container-grizzly2-http,2.25,jar,lib/serialtest,ACA5D2802C7D81E4350829696C96AEE2F01FE6A9
central,org.glassfish.grizzly,grizzly-http-server,2.3.28,jar,lib/serialtest,13BC9A63DAE3A0A623B52FE71753D5413D134540
central,org.glassfish.grizzly,grizzly-http,2.3.28,jar,lib/serialtest,BB34B4E7FBB66B53AC6D428DCC99F5925C9FF7BD
central,org.glassfish.grizzly,grizzly-framework,2.3.28,jar,lib/serialtest,23A90F6316B3776699B173CCF9394C69D15B7E9C

central,javax.validation,validation-api,1.1.0.Final,jar,lib/serialtest,8613AE82954779D518631E05DAA73A6A954817D5
central,javax.annotation,javax.annotation-api,1.2,jar,lib/serialtest,479C1E06DB31C432330183F5CAE684163F186146

central,org.javassist,javassist,3.20.0-GA,jar,lib/serialtest,A9CBCDFB7E9F86FBC74D3AFAE65F2248BFBF82A0

#xa libs:
central,org.apache.geronimo.specs,geronimo-jms_1.1_spec,1.1.1,jar,lib/xa,C872B46C601D8DC03633288B81269F9E42762CEA

#graphql
central,javax.servlet,javax.servlet-api,3.0.1,jar,lib/graphql,6BF0EBB7EFD993E222FC1112377B5E92A13B38DD
central,com.graphql-java,graphql-java,13.0,jar,lib/graphql,F3D5C387CF09A13922F719FC984E7B6CBF2A3CCF
central,com.graphql-java-kickstart,graphql-java-servlet,8.0.0,jar,lib/graphql,F0FEE2A1DD3715EE93473A54FA93F7CFDFC24E46
central,com.graphql-java,graphql-java-extended-scalars,1.0,jar,lib/graphql,D798F4D543331B26E5E0B359D84D775F66B0E3E0
central,org.skyscreamer,jsonassert,1.5.0,jar,lib/test,6C9D5FE2F59DA598D9AEFC1CFC6528FF3CF32DF3
central,com.vaadin.external.google,android-json,0.0.20131108.vaadin1,jar,lib/test,FA26D351FE62A6A17F5CDA1287C1C6110DEC413F
central,com.fasterxml.jackson.core,jackson-core,2.13.1,jar,lib/graphql,51AE921A2ED1E06CA8876F12F32F265E83C0B2B8
central,com.fasterxml.jackson.core,jackson-databind,2.13.1,jar,lib/graphql,698B2D2B15D9A1B7AAE025F1D9F576842285E7F6


# Copyright 2016 Goldman Sachs.
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

# Portions copyright Hiroshi Ito. Licensed under Apache 2.0 license
