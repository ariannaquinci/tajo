package org.apache.tajo.client;


import org.apache.tajo.TajoConstants;
import org.apache.tajo.conf.TajoConf;
import org.apache.tajo.exception.DuplicateDatabaseException;
import org.apache.tajo.service.ServiceTrackerFactory;
import org.apache.tajo.util.KeyValueSet;

import edu.emory.mathcs.backport.java.util.Arrays;
import org.apache.tajo.utils.QueryTestCaseBase;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.runners.Enclosed;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Collection;

@RunWith(value=Enclosed.class)
public class CatalogAdminClientImplTests {

    @RunWith(Parameterized.class)
    public static class createDataBaseTests {
        private boolean expectedException;
        private static TajoConf conf= new TajoConf();
        private String dbName;

        private CatalogAdminClientImpl catalogAdminClient= new CatalogAdminClientImpl(new SessionConnection(ServiceTrackerFactory.get(QueryTestCaseBase.getConf()), TajoConstants.DEFAULT_DATABASE_NAME,new KeyValueSet()));




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
}
