package org.apache.tajo.client;

import edu.emory.mathcs.backport.java.util.Arrays;
import org.apache.tajo.QueryTestCaseBase;
import org.apache.tajo.TajoConstants;
import org.apache.tajo.TajoTestingCluster;
import org.apache.tajo.TpchTestBase;
import org.apache.tajo.annotation.NotNull;
import org.apache.tajo.exception.NoSuchSessionVariableException;
import org.apache.tajo.exception.UndefinedDatabaseException;
import org.apache.tajo.service.ServiceTrackerFactory;
import org.apache.tajo.util.KeyValueSet;
import org.junit.*;
import org.junit.experimental.runners.Enclosed;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.stringtemplate.v4.ST;

import javax.validation.constraints.AssertTrue;
import java.util.*;

@RunWith(Enclosed.class)
public class SessionConnectionTests {
    protected static  HashMap map= new HashMap();
    static {
        map.put("ariannaVar", "ariannaVarValue");
    }


    @RunWith(Parameterized.class)
    public static class existSessionVariableTests{
        private static  SessionConnection sessionConnection;
        private final boolean expectedRes;

        private Class<? extends Exception> expectedException;
        private String varName;


        @BeforeClass
        public static void init() {

            sessionConnection= new SessionConnection(ServiceTrackerFactory.get(QueryTestCaseBase.getConf()), TajoConstants.DEFAULT_DATABASE_NAME, new KeyValueSet());
            sessionConnection.updateSessionVariables(map);
        }
        @AfterClass
        public static void clean() {
            sessionConnection.close();
        }
        @Parameterized.Parameters
        public static Collection<Object[]> getParameters() {
            return Arrays.asList(new Object[][] {
                    {"ariannaVar", true},
                    {"abc", false}

            });
        }
        //constructor
        public existSessionVariableTests(String varName, boolean res){
            this.varName=varName;
            this.expectedRes=res;
        }
        @Test
        public void existsSessionVarTests() {
            Assert.assertEquals( this.expectedRes, sessionConnection.existSessionVariable(varName));
        }
}
    @RunWith(Parameterized.class)
    public static class getSessionVariableTests{
        private static  SessionConnection sessionConnection;

        private boolean expectedException;
        private String varName;

        @BeforeClass
        public static void init() throws Exception {


            sessionConnection= new SessionConnection(ServiceTrackerFactory.get(QueryTestCaseBase.getConf()), TajoConstants.DEFAULT_DATABASE_NAME, new KeyValueSet());
            sessionConnection.updateSessionVariables(map);
        }
        @AfterClass
        public static void clean() {
            sessionConnection.close();
        }
        @Parameterized.Parameters
        public static Collection<Object[]> getParameters() {
            return Arrays.asList(new Object[][] {
                    {"ariannaVar", false},
                    {"abc", true}

            });
        }
        //constructor
        public getSessionVariableTests(String varName, boolean exc){
            this.varName=varName;
            this.expectedException=exc;

        }
        @Test
        public void getSessionVarTests() {
           try{
            Assert.assertEquals(map.get(this.varName), sessionConnection.getSessionVariable(varName));
           }catch (NoSuchSessionVariableException e){
               Assert.assertTrue(this.expectedException);
           }
        }
    }
    @RunWith(Parameterized.class)
    public static class getAllSessionVariableTests{
        private static  SessionConnection sessionConnection;

        private boolean exists;
        private String varName;

        @BeforeClass
        public static void init() throws Exception {


            sessionConnection= new SessionConnection(ServiceTrackerFactory.get(QueryTestCaseBase.getConf()), TajoConstants.DEFAULT_DATABASE_NAME, new KeyValueSet());
            sessionConnection.updateSessionVariables(map);
        }
        @AfterClass
        public static void clean() {
            sessionConnection.close();
        }
        @Parameterized.Parameters
        public static Collection<Object[]> getParameters() {
            return Arrays.asList(new Object[][] {
                    {"ariannaVar", true},
                    {"abc", false}

            });
        }
        //constructor
        public getAllSessionVariableTests(String varName, boolean exists){
            this.varName=varName;
            this.exists= exists;

        }
        @Test
        public void getAllSessionVarTests() {
            if(exists){
            Assert.assertTrue( sessionConnection.getAllSessionVariables().containsKey(varName));}
            else{
                Assert.assertFalse(sessionConnection.getAllSessionVariables().containsKey(varName));
            }

        }
    }



    @RunWith(Parameterized.class)
    public static class updateSessionVariableTests{
        private static  SessionConnection sessionConnection;

        private boolean expectedException;
        private String varName;
        private String varValue;

        @BeforeClass
        public static void init() throws Exception {


            sessionConnection= new SessionConnection(ServiceTrackerFactory.get(QueryTestCaseBase.getConf()), TajoConstants.DEFAULT_DATABASE_NAME, new KeyValueSet());

        }
        @AfterClass
        public static void clean() {
            sessionConnection.close();
        }
        @Parameterized.Parameters
        public static Collection<Object[]> getParameters() {
            return Arrays.asList(new Object[][] {
                    {"ariannaVar", "ariannaVal"},
                    {"abc", "abcVal"},
                    {"abc", "1"}

            });
        }
        //constructor
        public updateSessionVariableTests(String varName, String varValue){
            this.varName=varName;
            this.varValue=varValue;


        }
        @Test
        public void updateSessionVarTests() {
                Map testMap= new HashMap<>();
                testMap.put(varName, varValue);

                Assert.assertTrue( sessionConnection.updateSessionVariables(testMap).containsKey(varName) );
            }
    }


    @RunWith(Parameterized.class)
    public static class unsetSessionVariableTests{
        private static  SessionConnection sessionConnection;

        private static List<String> vars= new ArrayList();
        private String varName;
        @BeforeClass
        public static void initSession(){
            sessionConnection= new SessionConnection(ServiceTrackerFactory.get(QueryTestCaseBase.getConf()), TajoConstants.DEFAULT_DATABASE_NAME, new KeyValueSet());

        }
        @Before
        public void init() {


            sessionConnection.updateSessionVariables(map);
        }

        @AfterClass
        public static void clean() {
            sessionConnection.close();
        }
        @Parameterized.Parameters
        public static Collection<Object[]> getParameters() {

            return Arrays.asList(new Object[][] {
                    {"ariannaVar"},
                    {"abc"}

            });
        }
        //constructor
        public unsetSessionVariableTests(String varName){
            this.varName=varName;
            vars.add(varName);


        }
        @Test
        public void unsetSessionVarTests() {
           Assert.assertFalse( sessionConnection.unsetSessionVariables(this.vars).containsKey(this.varName));
        }
    }


    @RunWith(Parameterized.class)
    public static class getCurrentDBTests{
        private static  SessionConnection sessionConnection;

        private String db;
        private boolean exists;
        @BeforeClass
        public static void init(){
            sessionConnection = new SessionConnection(ServiceTrackerFactory.get(QueryTestCaseBase.getConf()), TajoConstants.DEFAULT_DATABASE_NAME, new KeyValueSet());

        }
        @Parameterized.Parameters
        public static Collection<Object[]> getParameters() {
            return Arrays.asList(new Object[][] {
                    {TajoConstants.DEFAULT_DATABASE_NAME, true},
                    {"notExistingDB", false},
                    {null, false}

            });
        }

        //constructor
        public getCurrentDBTests(String db, boolean exists) {
            this.db=db;
            this.exists=exists;
        }

        @AfterClass
        public static void clean() {
            sessionConnection.close();
        }


        @Test
        public void getCurrentDBTests() {
            if(exists){
                Assert.assertEquals(this.db, sessionConnection.getCurrentDatabase());}
            else{
                Assert.assertNotEquals(this.db, sessionConnection.getCurrentDatabase());
            }
        }
    }

    @RunWith(Parameterized.class)
    public static class selectDBTests{
        private static  SessionConnection sessionConnection;

        private String db;
        private boolean epectedException;
        @BeforeClass
        public static void init(){
            sessionConnection = new SessionConnection(ServiceTrackerFactory.get(QueryTestCaseBase.getConf()), TajoConstants.DEFAULT_DATABASE_NAME, new KeyValueSet());

        }
        @Parameterized.Parameters
        public static Collection<Object[]> getParameters() {
            return Arrays.asList(new Object[][] {
                    {TajoConstants.DEFAULT_DATABASE_NAME, false},
                    {"notExistingDB", true},
                    {null, true}

            });
        }

        //constructor
        public selectDBTests(String db, boolean exc) {
            this.db=db;
            this.epectedException=exc;
        }

        @AfterClass
        public static void clean() {
            sessionConnection.close();
        }


        @Test
        public void selectDBTests() {
          try{
              sessionConnection.selectDatabase(db);
              Assert.assertFalse(this.epectedException);
          }catch (UndefinedDatabaseException e) {
              Assert.assertTrue(this.epectedException);
          }
        }
    }


}





