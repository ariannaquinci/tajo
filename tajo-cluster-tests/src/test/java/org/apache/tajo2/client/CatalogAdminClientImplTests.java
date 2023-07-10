package org.apache.tajo2.client;


import net.bytebuddy.build.Plugin;
import org.apache.hadoop.fs.Path;
import org.apache.tajo.*;
import org.apache.tajo.catalog.Schema;
import org.apache.tajo.catalog.TableMeta;
import org.apache.tajo.catalog.partition.PartitionMethodDesc;
import org.apache.tajo.catalog.proto.CatalogProtos;
import org.apache.tajo.conf.TajoConf;
import org.apache.tajo.exception.*;
import org.apache.tajo.service.ServiceTrackerFactory;
import org.apache.tajo.storage.StorageUtil;
import org.apache.tajo.util.CommonTestingUtil;
import org.apache.tajo.util.KeyValueSet;

import edu.emory.mathcs.backport.java.util.Arrays;
import org.apache.tajo2.*;
import org.junit.*;
import org.junit.Assert;
import org.junit.experimental.runners.Enclosed;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Collection;

@RunWith(value=Enclosed.class)
public class CatalogAdminClientImplTests {

    @RunWith(Parameterized.class)
    public static class createDataBaseTests {
        private boolean expectedException;
        private String dbName;

        private static CatalogAdminClientImpl catalogAdminClient;

        @BeforeClass
        public static void init() {

            catalogAdminClient = new CatalogAdminClientImpl(new SessionConnection(ServiceTrackerFactory.get(QueryTestCaseBase.getConf()), TajoConstants.DEFAULT_DATABASE_NAME, new KeyValueSet()));

        }

        @After
        public void clean() throws UndefinedDatabaseException, InsufficientPrivilegeException, CannotDropCurrentDatabaseException, IOException {
             catalogAdminClient.dropDatabase(this.dbName);

        }
        @AfterClass
        public static void clear() throws IOException {
            catalogAdminClient.close();
        }


        @Parameterized.Parameters
        public static Collection<Object[][]> getParams() {
            return Arrays.asList(new Object[][]{
                    {"databaseExample", false},
                    {"dataBase with spaces name", false},
                    {null, false}
            });
        }

        //constructor
        public createDataBaseTests(String dbName, boolean exc) {
            this.dbName = dbName;
            this.expectedException = exc;
        }

        @Test
        public void createDataBaseTests() {
            try {
                catalogAdminClient.createDatabase(dbName);
                Assert.assertTrue(catalogAdminClient.existDatabase(dbName));
            } catch (DuplicateDatabaseException e) {
                Assert.assertTrue(this.expectedException);
            }catch(NullPointerException e){
                Assert.assertTrue(this.expectedException);
            }
        }
    }

    @RunWith(Parameterized.class)
    public static class createExternalTableTests {
        public static CatalogAdminClientImpl catalogAdminClient;
        private Class<? extends Exception> expectedException;
        private String tableName;
        private Schema schema;
        private PartitionMethodDesc partMethodDesc;
        private TableMeta meta;
        private URI path;
        private boolean created;

        @BeforeClass
        public static void init() throws IOException {

            catalogAdminClient = new CatalogAdminClientImpl(new SessionConnection(ServiceTrackerFactory.get(QueryTestCaseBase.getConf()), TajoConstants.DEFAULT_DATABASE_NAME, new KeyValueSet()));

        }

        @After
        public void dropTable() throws UndefinedTableException, InsufficientPrivilegeException {
            if (created) {
                catalogAdminClient.dropTable(this.tableName);
            }
        }

        @AfterClass
        public static void clean() throws UndefinedDatabaseException, InsufficientPrivilegeException, CannotDropCurrentDatabaseException, IOException {
            //cancella la tabella esterne esistenti

            catalogAdminClient.close();

        }

        //parameteres
        @Parameterized.Parameters
        public static Collection<Object[][]> getParams() {
            Schema schemaMock = BackendTestingUtil.mockupSchema;
            TableMeta metaMock = BackendTestingUtil.mockupMeta;


            return Arrays.asList(new Object[][]{
                  
                    //tableName              schema               path           meta         partMethodDescValid        expectedException
                    {"table1", schemaMock, false, metaMock, true, UnavailableTableLocationException.class},
                    {"table1", schemaMock, true, metaMock, true, null},
                    {"", schemaMock, true, metaMock, true, null},
                    {null, schemaMock, true, metaMock, true, NullPointerException.class},
                    {"table1", null, true, metaMock, true,NullPointerException.class},
                    {"table1", schemaMock, true, null, true, NullPointerException.class},
		{"table1", schemaMock, false, metaMock, true, NullPointerException.class},
                    {"table 1", schemaMock, true, metaMock, true, null},
                    //Il seguente test ha un comportamento inaspettato, con approccio whitebox vedrò perchè
                    //    {"table1", schemaMock, true, metaMock, false, NullPointerException.class},
                    //creo meta con nome tabella diverso da tableName e mi aspetto IllegalArgumentException, ma non è così
                    //  {"table1", schemaMock, true, metaMock, false, IllegalArgumentException.class},


            });
        }

        //constructor
        public createExternalTableTests(String tableName, Schema schema, boolean path, TableMeta meta, boolean partMethodDescValid, Class<? extends Exception> expectedExc) throws URISyntaxException, IOException {
            this.tableName = tableName;
            this.schema = schema;
            TajoTestingCluster cluster = TpchTestBase.getInstance().getTestingCluster();
            TajoConf conf = cluster.getConfiguration();
            if (path) {

                Path tPath = StorageUtil.concatPath(CommonTestingUtil.getTestDir(), "table1");
                BackendTestingUtil.writeTmpTable(conf, tPath);
                this.path = tPath.toUri();
            } else {
	   	 if(expectedExc==NullPointerException.class){
		    this.path=null;}
              	else{this.path = new URI("/target/example");}
            }


            this.meta = meta;

            this.expectedException = expectedExc;
            if (partMethodDescValid) {
                this.partMethodDesc = new PartitionMethodDesc("database", this.tableName, CatalogProtos.PartitionType.COLUMN, "expression", this.schema);
            } else {
                if(expectedExc.equals(IllegalArgumentException.class)) {
                    this.partMethodDesc = new PartitionMethodDesc("database", this.tableName + "bis", CatalogProtos.PartitionType.COLUMN, "expression", this.schema);
                }else{
                    this.partMethodDesc=null;
                }
            }
        }


        @Test
        public void createExternalTableTests() {

            try {
                Assert.assertEquals("default." + tableName, catalogAdminClient.createExternalTable(tableName, schema, path, meta, this.partMethodDesc).getName());
                created = true;
            } catch (UnavailableTableLocationException e) {
                Assert.assertEquals(this.expectedException, e.getClass());
            } catch (InsufficientPrivilegeException e) {
                Assert.assertEquals(this.expectedException, e.getClass());
            } catch (DuplicateTableException e) {
                Assert.assertEquals(this.expectedException, e.getClass());
            } catch (NullPointerException e) {
                Assert.assertEquals(this.expectedException, e.getClass());
            }
        }


    }
/*
    @RunWith(Parameterized.class)
    public static  class dropDatabaseTests{
        private Class<?extends Exception> expectedException;
        private String dbName;

        private static CatalogAdminClientImpl catalogAdminClient;

        @BeforeClass
        public static void initCatalogAdminClient() throws DuplicateDatabaseException{
            catalogAdminClient = new CatalogAdminClientImpl(new SessionConnection(ServiceTrackerFactory.get(QueryTestCaseBase.getConf()), TajoConstants.DEFAULT_DATABASE_NAME, new KeyValueSet()));
            catalogAdminClient.createDatabase("default_database");
        }

       
        @AfterClass
        public static void clear() throws IOException, UndefinedDatabaseException, InsufficientPrivilegeException, CannotDropCurrentDatabaseException {

            catalogAdminClient.close();
        }


        @Parameterized.Parameters
        public static Collection<Object[][]> getParams() {
            return Arrays.asList(new Object[][]{
                    {"default_database", null},
                    {"dataBaseNonEsistente", UndefinedDatabaseException.class},
                    {null, UndefinedDatabaseException.class},
                    //test added after whitebox approach
                  //  {"information_schema", InsufficientPrivilegeException.class}
            });
        }

        //constructor
        public dropDatabaseTests(String dbName, Class<? extends Exception> exc) {
            this.dbName = dbName;
            this.expectedException = exc;
        }

        @Test
        public void dropDataBaseTests() {
            try {

                catalogAdminClient.dropDatabase(dbName);
                Assert.assertFalse(catalogAdminClient.existDatabase(dbName));
                Assert.assertEquals(null, this.expectedException);

            } catch (UndefinedDatabaseException e) {
                Assert.assertEquals(this.expectedException, e.getClass());

            } catch (InsufficientPrivilegeException e) {
                Assert.assertEquals(this.expectedException, e.getClass());

            } catch (CannotDropCurrentDatabaseException e) {
                Assert.assertEquals(this.expectedException, e.getClass());

            }
        }

    }
    
*/
    @RunWith(Parameterized.class)
    public static class whiteboxCreateExternalTableTests{
        public static CatalogAdminClientImpl catalogAdminClient;
        private Class<? extends Exception> expectedException;
        private String tableName;
        private Schema schema;
        private PartitionMethodDesc partMethodDesc;
        private TableMeta meta;
        private URI path;
        private boolean created;

        @BeforeClass
        public static void init() throws IOException {

            catalogAdminClient = new CatalogAdminClientImpl(new SessionConnection(ServiceTrackerFactory.get(QueryTestCaseBase.getConf()), TajoConstants.DEFAULT_DATABASE_NAME, new KeyValueSet()));

        }

        @After
        public void dropTable() throws UndefinedTableException, InsufficientPrivilegeException {
            if (created) {
                catalogAdminClient.dropTable(this.tableName);
            }
        }

        @AfterClass
        public static void clean() throws UndefinedDatabaseException, InsufficientPrivilegeException, CannotDropCurrentDatabaseException, IOException {
            //cancella la tabella esterne esistenti

            catalogAdminClient.close();

        }
        @Parameterized.Parameters
        public static Collection<Object[][]> getParams() {
            Schema schemaMock = BackendTestingUtil.mockupSchema;
            TableMeta metaMock = BackendTestingUtil.mockupMeta;

            return Arrays.asList(new Object[][]{
                    //tableName              schema               path           meta        partitionMethodDescType      expectedException

                    //APPROCCIO WHITE-BOX
                    {"table1", schemaMock, true, metaMock, PartitionMethodDescTypes.ILLEGAL, null},
                    {"table1", schemaMock, true, metaMock, PartitionMethodDescTypes.NULL, null}
            });
        }

        //white box constructor
        public whiteboxCreateExternalTableTests(String tableName, Schema schema, boolean path, TableMeta meta, PartitionMethodDescTypes partMethodDescType, Class<? extends Exception> expectedExc) throws URISyntaxException, IOException {
            this.tableName = tableName;
            this.schema = schema;
            TajoTestingCluster cluster = TpchTestBase.getInstance().getTestingCluster();
            TajoConf conf = cluster.getConfiguration();
            if (path) {

                Path tPath = StorageUtil.concatPath(CommonTestingUtil.getTestDir(), "table1");
                BackendTestingUtil.writeTmpTable(conf, tPath);
                this.path = tPath.toUri();
            } else {
                this.path = new URI("/target/example");
            }


            this.meta = meta;

            this.expectedException = expectedExc;
            if (partMethodDescType== PartitionMethodDescTypes.LEGAL) {
                this.partMethodDesc = new PartitionMethodDesc("database", this.tableName, CatalogProtos.PartitionType.COLUMN, "expression", this.schema);
            } else if(partMethodDescType== PartitionMethodDescTypes.ILLEGAL) {
                this.partMethodDesc = new PartitionMethodDesc("database", this.tableName + "bis", CatalogProtos.PartitionType.COLUMN, "expression", this.schema);

            }else if(partMethodDescType== PartitionMethodDescTypes.NULL){
                this.partMethodDesc=null;
            }
        }
        @Test
        public void createExternalTableTests() {

            try {
                Assert.assertEquals("default." + tableName, catalogAdminClient.createExternalTable(tableName, schema, path, meta, this.partMethodDesc).getName());
                Assert.assertEquals(this.expectedException, null);
                created = true;
            } catch (UnavailableTableLocationException e) {
                Assert.assertEquals(this.expectedException, e.getClass());
            } catch (InsufficientPrivilegeException e) {
                Assert.assertEquals(this.expectedException, e.getClass());
            } catch (DuplicateTableException e) {
                Assert.assertEquals(this.expectedException, e.getClass());
            } catch (NullPointerException e) {
                Assert.assertEquals(this.expectedException, e.getClass());
            }
        }

    }



}

