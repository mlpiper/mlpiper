package com.parallelm.mlcomp;

import java.util.Map;

abstract class MCenterBaseComponent {
    protected MLOps mlops = null;
    protected Map<String, Object> params;
    protected boolean verbose = false;

    public MCenterBaseComponent() {
    }

    public MCenterBaseComponent(boolean verbose) {
        this.verbose = verbose;
    }

    public void setMLOps(MLOps mlops) {
        this.mlops = mlops;
    }

    public void configure(Map<String, Object> params) {
        this.params = params;

        if (verbose) {
            System.out.println("MCenterComponent.configure called");
            for (String key : params.keySet()) {
                System.out.println(String.format("Key: %s  Value: %s   Class: %s", key, this.params.get(key),
                        this.params.get(key).getClass()));
            }
        }
    }
}
