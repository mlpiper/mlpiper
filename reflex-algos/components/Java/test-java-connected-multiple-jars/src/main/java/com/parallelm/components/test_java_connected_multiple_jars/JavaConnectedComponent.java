package com.parallelm.components.test_java_connected_multiple_jars;

import com.parallelm.mlcomp.MCenterComponent;
import com.parallelm.components.test_java_connected_multiple_jars.JavaConnectedAddition;

import java.util.Arrays;
import java.util.ArrayList;
import java.util.List;

public class JavaConnectedComponent extends MCenterComponent
{
    @Override
    public List<Object> materialize(List<Object> parentDataObjects) throws Exception {
        System.out.println("Running JavaConnectedComponent");
        JavaConnectedAddition.printHello();

        if (mlops != null && mlops.isMLOpsLoaded()) {
            if (!mlops.isTestMode()) {
                throw new Exception("__test_mode__ must be set in systemConfig for this test");
            }
        }

        List<Object> outputs = new ArrayList<>();
        return outputs;
    }
}
