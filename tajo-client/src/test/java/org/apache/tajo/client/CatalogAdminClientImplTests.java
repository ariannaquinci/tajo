package org.apache.tajo.client;

import org.apache.tajo.catalog.Schema;
import org.apache.tajo.catalog.TableMeta;
import org.apache.tajo.catalog.partition.PartitionMethodDesc;
import org.apache.tajo.catalog.proto.CatalogProtos;
import org.apache.tajo.exception.DuplicateTableException;
import org.apache.tajo.exception.InsufficientPrivilegeException;
import org.apache.tajo.exception.UnavailableTableLocationException;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.runners.Enclosed;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import edu.emory.mathcs.backport.java.util.Arrays;
import java.net.URI;
import java.net.URISyntaxException;
import org.mockito.Mockito;

import java.util.Collection;

@RunWith(value= Enclosed.class)
public class CatalogAdminClientImplTests {

    @RunWith(Parameterized.class)
    public static class createExternalTableTests{
        private static CatalogAdminClientImpl catalogAdminClientImpl= new CatalogAdminClientImpl(Mockito.mock(SessionConnection.class));
        private String tableName;
        private TableMeta tableMeta;
        private PartitionMethodDesc partMethDesc=Mockito.mock(PartitionMethodDesc.class);
        private URI path;
        private Schema schema=Mockito.mock(Schema.class);

        private static TableMeta ValidMeta=new TableMeta(CatalogProtos.TableProto.getDefaultInstance());
        private static TableMeta inValidMeta= Mockito.mock(TableMeta.class);
        private Class<? extends Exception> expectedException;


        @Parameterized.Parameters
        public static Collection<Object[][]> getParameter() throws URISyntaxException {
            Mockito.when(inValidMeta.getProto()).thenReturn(CatalogProtos.TableProto.newBuilder().buildPartial());


            return Arrays.asList(new Object[][]{
                    //Unidimensional approach
                    // TABLE_NAME   SCHEMA_SIZE   PATH                              META    PARTITION_METHOD_DESC_VALID   SCHEMA_NULL   EXPECTED_EXCEPTION
                    {"table1",      0,            null,                             ValidMeta,              true          ,true           , UnavailableTableLocationException.class},
                    {"table 1",     0,            null,                             ValidMeta,              true          ,true          ,IllegalArgumentException.class},
                    {"table1",      0,            null,                             inValidMeta,              true,         false,       UnavailableTableLocationException.class     },
                    {"table1",      1,   new URI("./generate_source/table1"),   ValidMeta,              true,         false,            null},
                    {"table1",      -1,  new URI("./generate_source/table1"),   ValidMeta,              true,         false,            IllegalArgumentException.class},
                    {"table1",      5,  new URI("hkdf/hostName:8020/table1"),   ValidMeta,              true,         false      , InsufficientPrivilegeException.class},

            });
        }

        //constructor
        public createExternalTableTests(String tableName, int schema_size, URI path, TableMeta meta, boolean valid, boolean schema_null,    Class<? extends Exception>  exc){
            this.tableName=tableName;
            this.path=path;
            if(!schema_null) {
                Mockito.when(this.schema.size()).thenReturn(schema_size);
            }
            this.tableMeta=meta;

            if(valid){

                Mockito.when(  this.partMethDesc.getTableName()).thenReturn(this.tableName);
            }
            else{
                Mockito.when(  this.partMethDesc.getTableName()).thenReturn(this.tableName+"1");
            }
            this.expectedException=exc;
        }

        @Test
        public void createExternalTableTest(){
            try{

                Assert.assertEquals( catalogAdminClientImpl.createExternalTable(this.tableName,this.schema,this.path,this.tableMeta,this.partMethDesc).getName(),this.tableName);
            }catch(IllegalArgumentException e){
                Assert.assertEquals(e.getClass(), this.expectedException);
            }catch(InsufficientPrivilegeException e){
                Assert.assertEquals(e.getClass(), this.expectedException);
            }catch(UnavailableTableLocationException e){
                Assert.assertEquals(e.getClass(), this.expectedException);
            } catch (DuplicateTableException e) {
                Assert.assertEquals(e.getClass(), this.expectedException);
            }
        }
    }
}
