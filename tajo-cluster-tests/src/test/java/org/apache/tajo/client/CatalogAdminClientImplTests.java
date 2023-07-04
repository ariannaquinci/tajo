package org.apache.tajo.client;


import org.apache.hadoop.fs.Path;
import org.apache.orc.OrcProto;
import org.apache.tajo.*;
import org.apache.tajo.annotation.Nullable;
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
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.runners.Enclosed;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.mockito.Mockito;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Collection;
import java.util.List;

import static org.apache.tajo.conf.TajoConf.ConfVars.*;

@RunWith(value=Enclosed.class)
public class CatalogAdminClientImplTests {

    @RunWith(Parameterized.class)
    public static class createDataBaseTests {
        private boolean expectedException;
        private String dbName;

        private static  CatalogAdminClientImpl catalogAdminClient;
        @BeforeClass
        public static void init(){

            catalogAdminClient= new CatalogAdminClientImpl(new SessionConnection(ServiceTrackerFactory.get(QueryTestCaseBase.getConf()), TajoConstants.DEFAULT_DATABASE_NAME,new KeyValueSet()));

        }
        @AfterClass
        public static void clean() throws UndefinedDatabaseException, InsufficientPrivilegeException, CannotDropCurrentDatabaseException, IOException {
            List<String> databaseNames=catalogAdminClient.getAllDatabaseNames();
            for(String s: databaseNames){
                catalogAdminClient.dropDatabase(s);
            }
            catalogAdminClient.close();

        }



        @Parameterized.Parameters
        public static Collection<Object[][]> getParams(){
            return Arrays.asList(new Object[][]{
                    {"databaseExample", false},
                    {"dataBase not valid name", false},
                    {null, false}
                    });
        }
        //constructor
        public createDataBaseTests(String dbName, boolean exc){
            this.dbName=dbName;
            this.expectedException=exc;
        }
        @Test
        public void createDataBaseTests(){
            try{
                catalogAdminClient.createDatabase(dbName);
                Assert.assertTrue(catalogAdminClient.existDatabase(dbName));
            } catch (DuplicateDatabaseException e) {
                Assert.assertTrue(this.expectedException);
            }
        }
    }

    @RunWith(Parameterized.class)
    public static class createExternalTableTests{
        public static CatalogAdminClientImpl catalogAdminClient;
        private Class<? extends Exception> expectedException;
       private  String tableName;
       private Schema schema;
       private PartitionMethodDesc partMethodDesc;
       private TableMeta meta;
       private URI path;

        @BeforeClass
        public static void init() throws IOException {

            catalogAdminClient= new CatalogAdminClientImpl(new SessionConnection(ServiceTrackerFactory.get(QueryTestCaseBase.getConf()), TajoConstants.DEFAULT_DATABASE_NAME,new KeyValueSet()));

        }
        @AfterClass
        public static void clean() throws UndefinedDatabaseException, InsufficientPrivilegeException, CannotDropCurrentDatabaseException, IOException {
            //cancella la tabella esterne esistenti

            catalogAdminClient.close();

        }

        //parameteres
        @Parameterized.Parameters
        public static Collection<Object[][]> getParams() throws URISyntaxException, IOException {
            Schema schemaMock= BackendTestingUtil.mockupSchema;
            TableMeta metaMock= BackendTestingUtil.mockupMeta;


            return Arrays.asList(new Object[][]{
                   //tableName              schema               path           meta                 expectedException
             //       {"table1",              schemaMock     ,     false,         metaMock,            UnavailableTableLocationException.class},
               //     {"table1",              schemaMock     ,     true,          metaMock,            null},
                 //   {"",                    schemaMock     ,     true,          metaMock,            null},
                   // {null,                  schemaMock     ,     true,          metaMock,            NullPointerException.class},
                    {"table2",              schemaMock      ,    false,          metaMock,           InsufficientPrivilegeException.class}




            });
        }
        //constructor
        public createExternalTableTests(String tableName, Schema schema, boolean path, TableMeta meta,  Class<?extends Exception> expectedExc) throws URISyntaxException, IOException {
           this.tableName=tableName;
           this.schema= schema;
            TajoTestingCluster cluster = TpchTestBase.getInstance().getTestingCluster();
            TajoConf conf = cluster.getConfiguration();
           if(path){

               Path tPath=StorageUtil.concatPath(CommonTestingUtil.getTestDir(), "table1");
               BackendTestingUtil.writeTmpTable(conf, tPath);
               this.path= tPath.toUri();
           }else{
               if(expectedExc.equals(UnavailableTableLocationException.class)){
               this.path= new URI("/target/example");
               }else if(expectedExc.equals(InsufficientPrivilegeException.class)){
                   this.path=TajoConf.getWarehouseDir(conf).toUri();
               }

           }
           this.meta=meta;

           this.expectedException=expectedExc;
           this.partMethodDesc=new PartitionMethodDesc("database", this.tableName, CatalogProtos.PartitionType.COLUMN, "expression", this.schema);

        }


            @Test
        public void createExternalTableTests(){

            try{
                Assert.assertEquals("default."+tableName, catalogAdminClient.createExternalTable(tableName,schema,path, meta, this.partMethodDesc).getName());

            }catch (UnavailableTableLocationException e) {
                Assert.assertEquals( this.expectedException, e.getClass());
            } catch (InsufficientPrivilegeException e) {
                Assert.assertEquals( this.expectedException, e.getClass());
            } catch (DuplicateTableException e) {
                Assert.assertEquals(this.expectedException, e.getClass());
            }catch(NullPointerException e){
                Assert.assertEquals(this.expectedException, e.getClass());
            }
        }


    }
}
