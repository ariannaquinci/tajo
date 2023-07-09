package org.apache.tajo.client;

import edu.emory.mathcs.backport.java.util.Arrays;
import org.apache.tajo.QueryTestCaseBase;
import org.apache.tajo.*;
import org.apache.tajo.client.SessionConnection;
import org.apache.tajo.exception.NoSuchSessionVariableException;
import org.apache.tajo.exception.UndefinedDatabaseException;
import org.apache.tajo.service.ServiceTrackerFactory;
import org.apache.tajo.util.KeyValueSet;
import org.junit.*;
import org.junit.Assert;
import org.junit.experimental.runners.Enclosed;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.*;

@RunWith(Enclosed.class)
public class SessionConnectionTests {
    protected static  HashMap map= new HashMap();
    static {
        map.put("existingVar", "existingVarValue");
        //added after white-box approach
        map.put("variable1","value1");
        map.put("variable2","value2");
        map.put("variable3","value2");
        map.put("variable4","value2");
        map.put("variable5","value2");

    }


    @RunWith(Parameterized.class)
    public static class existSessionVariableTests{
        private static SessionConnection sessionConnection;
        private final boolean expectedRes;

        private Class<? extends Exception> expectedExc;
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
                    {"existingVar", true, null},
                    {"abc", false, null},
                    {null,false, NullPointerException.class}

            });
        }
        //constructor
        public existSessionVariableTests(String varName, boolean res, Class<?extends Exception> exc){
            this.varName=varName;
            this.expectedRes=res;
            this.expectedExc=exc;
        }
        @Test
        public void existsSessionVarTests() {
	try{
            Assert.assertEquals( this.expectedRes, sessionConnection.existSessionVariable(varName));
            }catch(NullPointerException e){
            Assert.assertEquals(e.getClass(), this.expectedExc);}
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
                    {"existingVar", false},
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
                Assert.assertFalse(this.expectedException);
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
                    {"existingVar", true},
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

        private String varName;
        private String varValue;

        @BeforeClass
        public static void init() {


            sessionConnection= new SessionConnection(ServiceTrackerFactory.get(QueryTestCaseBase.getConf()), TajoConstants.DEFAULT_DATABASE_NAME, new KeyValueSet());

        }
        @AfterClass
        public static void clean() {
            sessionConnection.close();
        }
        @Parameterized.Parameters
        public static Collection<Object[]> getParameters() {
            return Arrays.asList(new Object[][] {
                    {"existingVar", "existingVal"},
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

            Assert.assertTrue( sessionConnection.updateSessionVariables(testMap).containsValue(varValue) );
        }
    }


    public static class whiteboxUpdateSessionVariableTests{
        private static  SessionConnection sessionConnection;

        private static Map<String,String> testMap=new HashMap<>();

        @BeforeClass
        public static void init() {


            sessionConnection= new SessionConnection(ServiceTrackerFactory.get(QueryTestCaseBase.getConf()), TajoConstants.DEFAULT_DATABASE_NAME, new KeyValueSet());
            testMap.put("variable1", "value1bis");
            testMap.put("variable2", "value2bis");
            testMap.put("notExistingVariable", "notExistingValue");
        }
        @AfterClass
        public static void clean() {
            sessionConnection.close();
        }



        @Test
        public void wbupdateSessionVarTests() {
            Assert.assertTrue( sessionConnection.updateSessionVariables(testMap).containsValue("value1bis") );
        }
    }

    @RunWith(Parameterized.class)
    public static class unsetSessionVariableTests{
        private static  SessionConnection sessionConnection;

        private static List<String> vars= new ArrayList();
        private String varName;
        //white box addiction

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
                    {"existingVar"},
                    {"abc"},

            });
        }


        //constructor
        public unsetSessionVariableTests(String varName){
            this.varName=varName;
            vars.add(varName);}


        @Test
        public void unsetSessionVarTests() {
            Assert.assertFalse( sessionConnection.unsetSessionVariables(this.vars).containsKey(this.varName));
        }

    }


    @RunWith(Parameterized.class)
    public static class whiteboxUnsetSessionVariableTests{
        private static  SessionConnection sessionConnection;

        private  List<String> vars;

        @BeforeClass
        public static void initSession(){
            sessionConnection= new SessionConnection(ServiceTrackerFactory.get(QueryTestCaseBase.getConf()), TajoConstants.DEFAULT_DATABASE_NAME, new KeyValueSet());

        }
        @Before
        public void init() {


            sessionConnection.updateSessionVariables(map);
        }

        @After
        public void cleanList(){
            vars.clear();
        }
        @AfterClass
        public static void clean() {
            sessionConnection.close();
        }
        @Parameterized.Parameters
        public static Collection<Object[]> getParameters() {
            ArrayList<String> variables= new ArrayList<>();
            return Arrays.asList(new Object[][] {

                    //added after white-box approach
                    {"existingVar", "variable1", "variable2"},
                    {"existingVar", "variable1", "notExistingVar"}

            });
        }


        //white-box constructor
        public whiteboxUnsetSessionVariableTests(String variable1, String variable2, String variable3){
            vars= new ArrayList();
            vars.add(variable1);
            vars.add(variable2);
            vars.add(variable3);
        }
        @Test
        public void unsetSessionVarTests() {
            for(int i=0; i<vars.size(); i++){
                Assert.assertFalse( sessionConnection.unsetSessionVariables(this.vars).containsKey(this.vars.get(i)));}
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



