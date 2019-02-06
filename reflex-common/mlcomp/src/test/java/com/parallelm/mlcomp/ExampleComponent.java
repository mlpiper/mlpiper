package com.parallelm.mlcomp;

import java.util.ArrayList;
import java.util.List;


public class ExampleComponent extends MCenterComponent {

    @Override
    public List<Object> materialize(List<Object> parentDataObjects) throws Exception {
        // Getting the first object assuming it is a string and adding 3 to it and returning
        // it as the output
        int val = (int) parentDataObjects.get(0);
        List<Object> outputs = new ArrayList<>();
        outputs.add((val + 3));
        return outputs;
    }
}

