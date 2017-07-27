

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

package com.gs.fw.common.mithra.generator.objectxmlgenerator;

import com.gs.fw.common.mithra.MithraBusinessException;
import com.gs.fw.common.mithra.connectionmanager.LdapDataSourceProvider;
import com.gs.fw.common.mithra.databasetype.*;
import org.apache.tools.ant.BuildException;
import org.apache.tools.ant.Project;
import org.apache.tools.ant.Task;

import javax.sql.DataSource;
import java.io.*;
import java.sql.*;
import java.util.*;

public class MithraObjectXmlGenerator extends Task
{
    private static final String DB_NAME_POSTGRES = "postgres";
    private static final String DB_NAME_ORACLE = "oracle";
    private static final String DB_NAME_MARIA = "maria";
    private static final String DB_NAME_MSSQL = "mssql";
    private static final String DB_NAME_SYBASE = "sybase";
    private static final String DB_NAME_H2 = "h2";

    private static final String DB_NAME_UDB82 = "udb82";
    private static final String DB_NAME_SYBASEIQ = "sybaseiq";

    private static final Set<String> VALID_DBS = new HashSet<String>();

    static
    {
        VALID_DBS.add(DB_NAME_MSSQL);
        VALID_DBS.add(DB_NAME_H2);
        VALID_DBS.add(DB_NAME_POSTGRES);
        VALID_DBS.add(DB_NAME_SYBASE);
        VALID_DBS.add(DB_NAME_SYBASEIQ);
        VALID_DBS.add(DB_NAME_UDB82);
//        VALID_DBS.add(DB_NAME_MARIA);
        VALID_DBS.add(DB_NAME_ORACLE);
    }

    private boolean executed = false;
    private boolean excludeAsOfAttributesFromDbIndex = true;
    private String userName;
    private String password;
    private String schemaName;
    private String schema;
    private String catalog = null;
    private String driver;
    private String url;
    private String outputDir;
    private String databaseType;
    private DatabaseType dbType;
    private String ldapName;
    private String ldapDataSourceProviderClassName = "com.gs.fw.common.mithra.connectionmanager.JndiJdbcLdapDataSourceProvider";
    // START optional parameters for the xml generator
    private String generatedPackageName;
    private boolean toDateInclusive = true;
    private String infinityDateMethod = "com.gs.fw.common.mithra.util.DefaultInfinityTimestamp.getDefaultInfinity()";
    private String defaultIfNotSpecifiedMethod = "com.gs.fw.common.mithra.util.DefaultInfinityTimestamp.getDefaultInfinity()";
    private boolean generateTransactionalTables = true;
    private String processingDateTimezoneConversion = null;



    // END optional parameters for the xml generator

    private String includeTables;
    private String includeTablesFromFile;
    private List<String> includeList = null;

    private Map<String, TableInfo> tables = new HashMap<String, TableInfo>();
    private Map<String, TableInfo> views = new HashMap<String, TableInfo>();


    public boolean isExcludeAsOfAttributesFromDbIndex()
    {
        return excludeAsOfAttributesFromDbIndex;
    }

    public void setExcludeAsOfAttributesFromDbIndex(boolean excludeAsOfAttributesFromDbIndex)
    {
        this.excludeAsOfAttributesFromDbIndex = excludeAsOfAttributesFromDbIndex;
    }

    public String getLdapDataSourceProviderClassName()
    {
        return ldapDataSourceProviderClassName;
    }

    public void setLdapDataSourceProviderClassName(String ldapDataSourceProviderClassName)
    {
        this.ldapDataSourceProviderClassName = ldapDataSourceProviderClassName;
    }

    public String getDatabaseType()
    {
        return databaseType;
    }

    public void setDatabaseType(String databaseType)
    {
        this.databaseType = databaseType.toLowerCase();
    }

    public String getGeneratedPackageName()
    {
        return generatedPackageName;
    }

    public void setGeneratedPackageName(String generatedPackageName)
    {
        this.generatedPackageName = generatedPackageName;
    }

    public boolean isToDateInclusive()
    {
        return toDateInclusive;
    }

    public void setToDateInclusive(boolean toDateInclusive)
    {
        this.toDateInclusive = toDateInclusive;
    }

    public String getInfinityDateMethod()
    {
        return infinityDateMethod;
    }

    public void setInfinityDateMethod(String infinityDateMethod)
    {
        this.infinityDateMethod = infinityDateMethod;
    }

    public String getDefaultIfNotSpecifiedMethod()
    {
        return defaultIfNotSpecifiedMethod;
    }

    public void setDefaultIfNotSpecifiedMethod(String defaultIfNotSpecifiedMethod)
    {
        this.defaultIfNotSpecifiedMethod = defaultIfNotSpecifiedMethod;
    }

    public boolean isGenerateTransactionalTables()
    {
        return generateTransactionalTables;
    }

    public void setGenerateTransactionalTables(boolean generateTransactionalTables)
    {
        this.generateTransactionalTables = generateTransactionalTables;
    }

    public String getProcessingDateTimezoneConversion()
    {
        return processingDateTimezoneConversion;
    }

    public void setProcessingDateTimezoneConversion(String processingDateTimezoneConversion)
    {
        this.processingDateTimezoneConversion = processingDateTimezoneConversion;
    }

    public String getIncludeTablesFromFile()
    {
        return includeTablesFromFile;
    }

    public void setIncludeTablesFromFile(String includeTablesFromFile)
    {
        this.includeTablesFromFile = includeTablesFromFile;

        // Check if a list is already set
        if (includeList != null)
        {
            return;
        }
        else
        {
            includeList = new ArrayList<String>();
        }

        if (includeTablesFromFile != null)
        {
            BufferedReader reader;

            try
            {
                reader = new BufferedReader(new FileReader(new File(includeTablesFromFile)));
            }
            catch (FileNotFoundException e)
            {
                this.log("could not open file " + includeTablesFromFile, Project.MSG_ERR);

                includeTables = null;
                return;
            }

            try
            {
                String line;

                while ((line = reader.readLine()) != null)
                {
                    includeList.add(line);
                }
            }
            catch (IOException e)
            {
                this.log("could not read file", Project.MSG_ERR);
                includeTables = null;
            }

            try
            {
                reader.close();
            }
            catch (IOException e)
            {
                this.log("could not close reader", Project.MSG_ERR);
            }
        }
    }

    public String getIncludeTables()
    {
        return includeTables;
    }

    public void setIncludeList(List<String> includeList)
    {
        this.includeList = includeList;
    }

    public void setIncludeTables(String includeTables)
    {
        if (includeList != null)
        {
            return;
        }
        else
        {
            includeList = new ArrayList<String>();
        }

        this.includeTables = includeTables;

        String[] split = includeTables.split(",");

        for (int i = 0; i < split.length; i++)
        {
            split[i] = split[i].trim();
            includeList.add(split[i]);
        }
    }

    public String getUserName()
    {
        return userName;
    }

    public void setUserName(String userName)
    {
        this.userName = userName;
    }

    public String getLdapName()
    {
        return ldapName;
    }

    public void setLdapName(String ldapName)
    {
        this.ldapName = ldapName;
    }

    public String getCatalog()
    {
        return catalog;
    }

    public void setCatalog(String catalog)
    {
        this.catalog = catalog;
    }

    public boolean isExecuted()
    {
        return executed;
    }

    public void setExecuted(boolean executed)
    {
        this.executed = executed;
    }

    public String getPassword()
    {
        return password;
    }

    public void setPassword(String password)
    {
        this.password = password;
    }

    public String getSchema()
    {
        return schema;
    }

    public void setSchema(String schema)
    {
        this.schema = schema;
    }

    public String getDriver()
    {
        return driver;
    }

    public void setDriver(String driver)
    {
        this.driver = driver;
    }

    public String getUrl()
    {
        return url;
    }

    public void setUrl(String url)
    {
        this.url = url;
    }

    public String getOutputDir()
    {
        return outputDir;
    }

    public void setOutputDir(String outputDir)
    {
        this.outputDir = outputDir;
    }

    @Override
    public void execute() throws BuildException
    {
        if (!executed)
        {
            this.processTableInfo();
            this.log("writing xml files.", Project.MSG_DEBUG);
            this.generateMithraObjectXml();
        }
    }

    private void checkOutputDirSpecified()
    {
        if (outputDir == null)
        {
            throw new MithraBusinessException("No outputDir specified. Please specify outputDir.");
        }
        if (!new File(outputDir).exists())
        {
            throw new MithraBusinessException("Specified outputDir does not exist: " + outputDir);
        }
    }

    public Map<String, TableInfo> processTableInfo()
    {
        Connection connection = null;
        try
        {
            initialize();
            connection = getConnection();

            DatabaseMetaData metaData = connection.getMetaData();
            this.log("getting table information.", Project.MSG_DEBUG);
            ArrayList<String> tableNameList = this.getTableNameListByTypes(metaData, includeList, new String[]{"TABLE"});
            this.populateTableInfoMap(metaData, tableNameList);
            return this.tables;
        }
        catch (SQLException e)
        {
            this.log(e.getMessage(), Project.MSG_ERR);
            throw new BuildException();
        }
        finally
        {
            this.closeConnection(connection);
        }

    }

    public Map<String, TableInfo> processViewInfo()
    {
        Connection connection = null;
        try
        {
            initialize();
            connection = getConnection();

            DatabaseMetaData metaData = connection.getMetaData();
            this.log("getting view information.", Project.MSG_DEBUG);
            ArrayList<String> viewList = this.getTableNameListByTypes(metaData, includeList, new String[]{"VIEW"});
            this.populateViewInfoMap(metaData, viewList);
            return views;
        }
        catch (SQLException e)
        {
            this.log(e.getMessage(), Project.MSG_ERR);
            throw new BuildException();
        }
        finally
        {
            this.closeConnection(connection);
        }
    }

    private Connection getConnection()
    {
        this.log("establishing connection", Project.MSG_DEBUG);
        Connection connection = this.establishConnection(getDriver(), getUrl(), getUserName(), getPassword());

        if (connection == null)
        {
            throw new BuildException("Connection is null");
        }
        return connection;
    }

    private void initialize()
    {
        List<String> initializationErrors = this.checkForArguments();

        if (!initializationErrors.isEmpty())
        {
            int size = initializationErrors.size();
            for (int i = 0; i < size; i++)
            {
                this.log(initializationErrors.get(i), Project.MSG_ERR);
            }
            throw new MithraBusinessException("Could not initialize Mithra Object XML generator. See exceptions above.");
        }

        this.initializeObjectXmlGenerator();
    }

    private void initializeObjectXmlGenerator()
    {
        this.initializeDbType();
        this.initializeSchemaOrCatalog();
    }

    private void initializeSchemaOrCatalog()
    {
        if (databaseType.equalsIgnoreCase(DB_NAME_SYBASE) || databaseType.equalsIgnoreCase(DB_NAME_POSTGRES) || databaseType.equalsIgnoreCase(DB_NAME_MARIA) || databaseType.equalsIgnoreCase(DB_NAME_MSSQL))
        {
            this.schemaName = null;
            this.catalog = schema;
        }
        else  // for both DB2 and Sybase IQ
        {
            this.schemaName = schema;
            this.catalog = null;
        }
    }

    private void initializeDbType()
    {
        if (!VALID_DBS.contains(databaseType))
        {
            this.log("Invalid database type specified. The valid values for this attribute are: "+VALID_DBS.toString(), Project.MSG_ERR);
            throw new BuildException("Invalid database type specified.");
        }
        if (databaseType.equalsIgnoreCase(DB_NAME_SYBASE))
        {
            dbType = SybaseDatabaseType.getInstance();
        }
        else if (databaseType.equalsIgnoreCase(DB_NAME_UDB82))
        {
            dbType = Udb82DatabaseType.getInstance();
        }
        else if (databaseType.equalsIgnoreCase(DB_NAME_SYBASEIQ))
        {
            dbType = SybaseIqDatabaseType.getInstance();
        }
        else if (databaseType.equalsIgnoreCase(DB_NAME_H2))
        {
            dbType = H2DatabaseType.getInstance();
        }
        else if (databaseType.equalsIgnoreCase(DB_NAME_POSTGRES))
        {
            dbType = PostgresDatabaseType.getInstance();
        }
        else if (databaseType.equalsIgnoreCase(DB_NAME_MARIA))
        {
            dbType = MariaDatabaseType.getInstance();
        }
        else if (databaseType.equalsIgnoreCase(DB_NAME_ORACLE))
        {
            dbType = OracleDatabaseType.getInstance();
        }
        else if (databaseType.equalsIgnoreCase(DB_NAME_MSSQL))
        {
            dbType = MsSqlDatabaseType.getInstance();
        }
    }

    private List<String> checkForArguments()
    {
        List<String> initializationErrors = new ArrayList<String>(5);
        if (userName == null)
        {
            initializationErrors.add("No userName specified. Please specify database userName.");
        }
        if (password == null)
        {
            initializationErrors.add("No password specified. Please specify password.");
        }
        if ((driver == null || url == null) && (ldapName == null))
        {
            initializationErrors.add("Either driver and url or ldapName must be specified.");
        }
        if (url != null && ldapName != null)
        {
            initializationErrors.add("Both url and ldapName are specified. Only one of two can be specified");
        }
        if (databaseType == null)
        {
            initializationErrors.add("No databaseType specified. Please specify databaseType. The valid values for this attribute are: "+VALID_DBS.toString());
        }
        return initializationErrors;
    }

    private void populateViewInfoMap(DatabaseMetaData metaData, ArrayList<String> tableNames)
    {
        for (String tableName : tableNames)
        {
            TableInfo tableInfo = views.get(tableName);
            if (tableInfo == null)
            {
                tableInfo = new TableInfo();
                tableInfo.setTableName(tableName);
                views.put(tableName, tableInfo);
            }
            tableInfo.populateColumnList(metaData, catalog, schemaName, dbType);
        }
    }

    private void populateTableInfoMap(DatabaseMetaData metaData, ArrayList<String> tableNames)
    {
        for (String tableName : tableNames)
        {
            TableInfo tableInfo = tables.get(tableName);
            if (tableInfo == null)
            {
                tableInfo = new TableInfo();
                tableInfo.setTableName(tableName);
                tables.put(tableName, tableInfo);
            }
            tableInfo.populateTableInfoWithMetaData(metaData, catalog, schemaName, dbType, excludeAsOfAttributesFromDbIndex);
        }
        buildCrossLinkingReferences();
    }

    private void buildCrossLinkingReferences()
    {
    	Set<String> tableNames = tables.keySet();
        for(TableInfo t : tables.values())
    	{
    		t.removeForeignKeysForMissingTables(tableNames);
            for(ForeignKey fk : t.getForeignKeys().values())
    		{
    			String refTable = fk.getRefTableName();
    			TableInfo tableB = tables.get(refTable);
    			fk.setTableB(tableB);
    			fk.initMultipleRelationsBetweenTables();
    		}
    		if(t.isManyToManyTable())
    		{
    			this.log("Table " + t.getTableName() + ": is Many-to-Many.");
    			// Many-to-Many table has two foreign keys
    			ForeignKey[] fks = t.getForeignKeys().values().toArray(new ForeignKey[2]);
    			TableInfo tableA = fks[0].getTableB();
    			TableInfo tableB = fks[1].getTableB();

    			ForeignKey aToB = new ForeignKey(tableA, tableA.getTableName() + "-" + tableB.getTableName(),
                        tableB.getTableName());
    			aToB.setTableB(tableB);
    			aToB.setTableAB(t);
    			tableA.getForeignKeys().put(aToB.getName(), aToB);
    		}
    	}
    }

    private ArrayList<String> getTableNameListByTypes(DatabaseMetaData metaData, List includeList, String[] types)
    {
        ArrayList<String> tables = new ArrayList<String>();
        ResultSet rs = null;

        try
        {
            rs = metaData.getTables(catalog, schemaName, "%", types);

            while (rs.next())
            {
                String str = rs.getString("TABLE_NAME");

                if ((includeList == null) || (includeList.contains(str)))
                {
                    tables.add(str);
                }
            }
        }
        catch (SQLException e)
        {
            this.log(e.getMessage(), Project.MSG_ERR);
            throw new BuildException(e);
        }
        finally
        {
            closeResultSet(rs);
        }
        return tables;
    }

    private Connection establishConnection(String driver, String url, String userName, String password)
    {

        try
        {
            if (ldapName == null)
            {
                Class.forName(driver).newInstance();
                return DriverManager.getConnection(url, userName, password);
            }
            else
            {
                Properties env = new Properties();
                env.put("com.gs.fw.aig.jdbc.useConnectionPool", "false");
                env.put("user", userName);
                env.put("password", password);
                if (this.getDatabaseType().contains("sybase"))
                {
                    env.put("com.gs.fw.aig.jdbc.global.DataSourceImpl","com.sybase.jdbc4.jdbc.SybDataSource");
                }
                LdapDataSourceProvider ldapDataSourceProvider;
                try
                {
                    ldapDataSourceProvider = (LdapDataSourceProvider) Class.forName(ldapDataSourceProviderClassName).newInstance();
                }
                catch (Exception e)
                {
                    throw new RuntimeException("could not create LDAP data source ", e);
                }
                DataSource ds = ldapDataSourceProvider.createLdapDataSource(env, ldapName);;
                return ds.getConnection(userName, password);
            }
        }
        catch (SQLException e)
        {
            this.log(e.getClass().getName() + ": " + e.getMessage(), e, Project.MSG_ERR);
            return null;
        }
        catch (Exception e)
        {
            this.log(e.getClass().getName() + ": " + e.getMessage(), e, Project.MSG_ERR);
            return null;
        }
    }

    private void generateMithraObjectXml() throws BuildException
    {
        this.checkOutputDirSpecified();

        for (TableInfo table : tables.values())
        {
            try
            {
                PrintWriter writer = new PrintWriter(new BufferedWriter(new FileWriter(new File(outputDir + "/" + table.getClassName() + ".xml"))));

                if (this.generateTransactionalTables)
                {
                    writer.println("<MithraObject objectType= \"transactional\">");
                }
                else
                {
                    writer.println("<MithraObject>");
                }

                writer.println();

                String packageName = getGeneratedPackageName() != null ? getGeneratedPackageName() : "INSERT PACKAGE HERE";
                writer.print("    <PackageName>" + packageName);
                writer.println("</PackageName>");

                writer.print("    <ClassName>");
                writer.print(table.getClassName());
                writer.println("</ClassName>");

                writer.print("    <DefaultTable>");
                writer.print(table.getTableName());
                writer.println("</DefaultTable>");

                writer.println();

                if (table.getPkList().size() == 0)
                {
                    writer.println();
                    writer.println("NO PRIMARY KEY FOR THIS TABLE. BE SURE TO FIX DATABASE AND THIS FILE!");
                    writer.println();
                }

                this.printAttributes(writer, table);

                writer.println();

                printRelationships(writer, table);
                writer.println();

                this.printIndices(writer, table);

                writer.println();

                writer.println("</MithraObject>");

                writer.close();
            }
            catch (IOException e)
            {
                this.log(e.getMessage(), Project.MSG_ERR);
                throw new BuildException();
            }
        }
    }

    private void printRelationships(PrintWriter writer, TableInfo tableInfo)
    {
        if (tableInfo.isManyToManyTable())
            return;

        for (ForeignKey fk : tableInfo.getForeignKeys().values())
        {
            String relationshipName = fk.getRelationshipName();
            String reverseRelationshipName = fk.getReverseRelationshipName();
            String cardinality = fk.getTableAMultiplicity() + "-to-" + fk.getTableBMultiplicity();
            String joinOperation = fk.getJoinOperation();
            writer.println("    <Relationship name=\"" + relationshipName + "\" relatedObject=\"" + fk.getTableB().getClassName() + "\" cardinality=\"" + cardinality + "\""
                           + " reverseRelationshipName=\"" + reverseRelationshipName + "\">");
            writer.println("        " + joinOperation);
            writer.println("    </Relationship>");
        }
    }

    public void printAttributes(PrintWriter writer, TableInfo tableInfo)
    {
        Map<String, ColumnInfo> columnList = tableInfo.getColumnMap();
        List<ColumnInfo> asOfList = tableInfo.getAsOfList();

        for (ColumnInfo anAsOfList : asOfList)
        {
            this.printAsOfAttribute(anAsOfList, writer);
        }

        for (ColumnInfo column : columnList.values())
        {
            if (!column.isAsOfAttribute())
            {
                this.printAttribute(column, writer);
            }
        }
    }

    private void printAttribute(ColumnInfo column, PrintWriter writer)
    {
        writer.print("    <Attribute name=\"" + column.getAttributeName() + "\" ");
        String javaType = dbType.getJavaTypeFromSql(column.getJavaType().trim().toLowerCase(),
                                                    column.getPrecision(), column.getDecimal());

        if (javaType == null)
        {
            writer.print("javaType=\"" + column.getJavaType() + "\" ");
        }
        else
        {
            writer.print("javaType=\"" + javaType + "\" ");
        }

        if (("String".equals(javaType)) && (column.getColumnSize() != -1))
        {
            writer.print("maxLength=\"" + column.getColumnSize() + "\" ");
        }

        if (column.isPartOfPk())
        {
            writer.print("primaryKey=\"true\" ");
        }

        if (column.isIdentity())
        {
            writer.print("identity=\"true\" ");
        }

        writer.print("columnName=\"" + column.getColumnName() + "\" ");

        if(!column.isNullable() && !column.isPartOfPk())
        {
            writer.print("nullable=\"" + column.isNullable() + "\" ");
        }

        if ("BigDecimal".equals(javaType))
        {
            writer.print("precision=\"" + column.getPrecision() + "\" ");
            writer.print("scale=\"" + column.getDecimal() + "\" ");
        }

        writer.println("/>");
    }

    private void printAsOfAttribute(ColumnInfo column, PrintWriter writer)
    {
        writer.print("    <AsOfAttribute name=\"" + column.getAsOfAttributeName() + "\" ");
        writer.print("fromColumnName=\"" + column.getAsOfColumnFrom().getColumnName() + "\" ");
        writer.print("toColumnName=\"" + column.getAsOfColumnTo().getColumnName() + "\" ");

        if (column.isPartOfPk())
        {
            writer.print("primaryKey=\"true\" ");
        }

        if ("businessDate".equals(column.getAsOfAttributeName()))
        {
            writer.print("toIsInclusive=\"" + this.toDateInclusive + "\" ");
            writer.print("infinityDate=\"[" + this.infinityDateMethod + "]\" ");
        }
        else
        {
            writer.print("toIsInclusive=\"" + this.toDateInclusive + "\" ");
            writer.print("infinityDate=\"[" + this.infinityDateMethod + "]\" ");
            writer.print("defaultIfNotSpecified=\"[" + this.defaultIfNotSpecifiedMethod + "]\" ");
            writer.print("isProcessingDate=\"true\" ");
            //timezoneConversion="convert-to-utc"
            if (this.processingDateTimezoneConversion != null)
            {
                writer.print("timezoneConversion=\"" + this.processingDateTimezoneConversion + "\" ");
            }
        }
        writer.println("/>");
    }

    public void printIndices(PrintWriter writer, TableInfo tableInfo)
    {
        for (Iterator it = tableInfo.getIndexMap().values().iterator(); it.hasNext();)
        {
            IndexInfo indexInfo = (IndexInfo) it.next();

            /* Only output unique indices */
            if (!indexInfo.isUnique())
            {
                continue;
            }

            writer.print("<Index name=\"" + indexInfo.getName() + "\" unique=\"true\">");

            for (int i = 0; i < indexInfo.getColumnInfoList().size(); i++)
            {
                writer.print((indexInfo.getColumnInfoList().get(i)).getAttributeName());

                if (i < indexInfo.getColumnInfoList().size() - 1)
                {
                    writer.print(", ");
                }
            }
            writer.println("</Index>");
        }
    }

    private void closeResultSet(ResultSet rs)
    {
        if (rs != null)
        {
            try
            {
                rs.close();
            }
            catch (SQLException e)
            {
                this.log(e.getMessage(), Project.MSG_ERR);
                throw new BuildException();
            }
        }
    }

    private void closeConnection(Connection connection)
    {
        if (connection != null)
        {
            try
            {
                connection.close();
            }
            catch (SQLException e)
            {
                this.log(e.getMessage(), Project.MSG_ERR);
                throw new BuildException();
            }
        }
    }


    protected static String getCredential(String key, Properties credentials)
    {
        String property = credentials.getProperty(key);
        if (property == null)
        {
            throw new RuntimeException("Missing property: "+key);
        }
        return property;
    }

    public static void main(String[] args)
    {
        System.out.println("Not a real main method. For testing only");
        Properties credentials = new Properties();
        try
        {
            credentials.load(MithraObjectXmlGenerator.class.getClassLoader().getResourceAsStream("credentials.properties"));
        }
        catch (IOException e)
        {
            throw new RuntimeException("No credentials found");
        }
        MithraObjectXmlGenerator generator = new MithraObjectXmlGenerator();
        generator.setUserName(getCredential("postgres_user", credentials));
        generator.setPassword(getCredential("postgres_password", credentials));
        generator.setUrl("jdbc:postgresql://"+getCredential("postgres_hostName", credentials)+":"+
                getCredential("postgres_port", credentials)+"/"+getCredential("postgres_databaseName", credentials));
        generator.setDatabaseType("postgres");
        generator.setDriver("org.postgresql.Driver");
        generator.setOutputDir("./reladomogenutil/target/reverse/postgres");
        generator.setSchema(getCredential("postgres_schemaName", credentials));
        generator.setIncludeTables("ALL_TYPES, IDENTITY_TABLE, PRODUCT, TEST_BALANCE_NO_ACMAP, ORDERS, ORDER_ITEM");

        generator.execute();
    }
}