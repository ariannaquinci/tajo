package org.apache.tajo2.client;

import edu.emory.mathcs.backport.java.util.Arrays;
import org.apache.tajo.TajoConstants;
import org.apache.tajo.exception.DuplicateDatabaseException;
import org.apache.tajo.exception.SQLSyntaxError;
import org.apache.tajo.exception.TajoException;
import org.apache.tajo.exception.UndefinedDatabaseException;
import org.apache.tajo.service.ServiceTrackerFactory;
import org.apache.tajo.util.KeyValueSet;
import org.apache.tajo2.QueryTestCaseBase;
import org.junit.*;
import org.junit.experimental.runners.Enclosed;
import org.junit.rules.Timeout;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import org.apache.tajo.error.Errors;

import javax.validation.constraints.Null;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collection;

@RunWith(Enclosed.class)
public class QueryClientImplTests {

    @RunWith(Parameterized.class)
    public static class executeQueryTests{
        private String query;
        private static QueryClientImpl queryClient;
        private Errors.ResultCode result;

        @BeforeClass
        public static void init(){
            queryClient= new QueryClientImpl(new SessionConnection(ServiceTrackerFactory.get(QueryTestCaseBase.getConf()), TajoConstants.DEFAULT_DATABASE_NAME, new KeyValueSet()));

        }
        @AfterClass
        public static void clear(){
            queryClient.executeQuery("DROP TABLE myTable;");
            queryClient.close();
        }
        @Parameterized.Parameters

        public static Collection<Object[][]> getParams() {
            return Arrays.asList(new Object[][]{
                    //valid query
                    {"CREATE TABLE myTable (val1 char, val2 int);", Errors.ResultCode.OK},

                    //invalid query
                    {"invalidQueryString;", Errors.ResultCode.SYNTAX_ERROR},

                    //valid query not existing table
                    {"INSERT INTO newTable values ('a', 3);",Errors.ResultCode.UNDEFINED_TABLE},
                    {null,null}
            });
        }

        //constructor
        public executeQueryTests(String query, Errors.ResultCode resCode){
            this.query=query;
            this.result=resCode;
        }
        @Test
        public void executeQueryTest(){
            try {
                Assert.assertEquals(this.result, queryClient.executeQuery(query).getState().getReturnCode());
            }catch (NullPointerException e){
                Assert.assertEquals(this.result,null);
            }
        }

    }

    @RunWith(Parameterized.class)
    public static class whiteboxExecuteQueryAndGetResultTests{
        private String query;
        private static QueryClientImpl queryClient;
        private Class<?extends  Exception> exception;


        @BeforeClass
        public static void init() throws DuplicateDatabaseException, UndefinedDatabaseException {
            queryClient= new QueryClientImpl(new SessionConnection(ServiceTrackerFactory.get(QueryTestCaseBase.getConf()), TajoConstants.DEFAULT_DATABASE_NAME, new KeyValueSet()));

            queryClient.executeQuery("CREATE TABLE myTable (val1 char, val2 int);");
            queryClient.executeQuery("INSERT INTO myTable values ('T',1);");
        }
        @AfterClass
        public static void clear(){

            queryClient.executeQuery("DROP TABLE myTable;");
            queryClient.close();
        }
        @Parameterized.Parameters

        public static Collection<Object[][]> getParams() {
            return Arrays.asList(new Object[][]{

                    //valid query, wrong type of entry
                    {"INSERT INTO myTable values ('a', 3);", NullPointerException.class},
                    //invalid query
                    {"invalidQueryString", SQLSyntaxError.class},
                    {null, NullPointerException.class},
                     //valid query with non null result
                    {"SELECT val2 FROM myTable;", null},
                    //ADDED AFTER WHITE BOX APPROACH
                    //query to be fetched
                    //il seguente test va in loop
               //     {"SELECT val2 FROM myTable WHERE val1='T';", null},

            });
        }

        //constructor
        public whiteboxExecuteQueryAndGetResultTests(String query, Class<?extends  Exception> exception){
            this.query=query;
            this.exception=exception;
        }
        @Test
        public void executeQueryAndGetResultTest() throws SQLException {
          try{

              ResultSet res=queryClient.executeQueryAndGetResult(this.query);
              res.next();
               Assert.assertEquals(res.getInt(1), 1);
               Assert.assertEquals(this.exception, null);
           }
           catch(TajoException e){
              Assert.assertEquals(this.exception, e.getClass());
          }catch(NullPointerException e){
              Assert.assertEquals(this.exception, e.getClass());
          }
        }

    }


}
