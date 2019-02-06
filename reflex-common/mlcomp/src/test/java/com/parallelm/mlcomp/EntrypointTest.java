package com.parallelm.mlcomp;

import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;

import java.util.ArrayList;
import java.util.List;


/**
 * Unit test for simple PythonStandaloneComponent.
 */
public class EntrypointTest
    extends TestCase
{
    /**
     * Create the test case
     *
     * @param testName name of the test case
     */
    public EntrypointTest(String testName )
    {
        super( testName );
    }

    /**
     * @return the suite of tests being tested
     */
    public static Test suite()
    {
        return new TestSuite( EntrypointTest.class );
    }

    /**
     * Rigourous Test :-)
     */
    public void testEntryPoint() throws Exception
    {
        ComponentEntryPoint entryPoint =  new ComponentEntryPoint("com.parallelm.mlcomp.ExampleComponent");
        MCenterComponent comp = (MCenterComponent) entryPoint.getComponent();

        List<Object>objList = new ArrayList<>();
        objList.add(1);
        List<Object> outputs = comp.materialize(objList);
        int retVal = (int) outputs.get(0);
        assertEquals(4, retVal);
    }
}
