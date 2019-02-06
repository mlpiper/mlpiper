package com.parallelm.mlcomp;

import java.io.IOException;

public abstract class MCenterRestfulComponent extends MCenterBaseComponent {

    public class Result {
        public int returned_code;
        public String json;
    }

    public abstract void loadModel(String modelPath) throws IOException;

    public abstract Result predict(String query_string, String body_data) throws Exception;
}
