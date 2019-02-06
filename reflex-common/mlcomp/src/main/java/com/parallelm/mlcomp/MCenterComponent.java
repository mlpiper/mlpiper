package com.parallelm.mlcomp;

import java.util.List;

abstract public class MCenterComponent extends MCenterBaseComponent {

    public MCenterComponent() {
    }

    public MCenterComponent(boolean verbose) {
        super(verbose);
    }

    public abstract List<Object> materialize(List<Object> parentDataObjects) throws Exception;
}
