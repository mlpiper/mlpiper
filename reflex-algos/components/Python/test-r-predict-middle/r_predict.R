#!/usr/bin/env Rscript

# Required for reticulate to work properly
Sys.setenv(MKL_THREADING_LAYER = "GNU")

library("reticulate")
Sys.getenv(c("PYTHONPATH"))
mlcomp_python = Sys.getenv(c("MLCOMP_PYTHON"))
print(paste0("python: ", mlcomp_python))
if (mlcomp_python != "") {
    use_python(mlcomp_python)
}
py_config()

mlops <- import("parallelm.mlops", convert = TRUE)
mlops <- mlops$mlops
print("After import of mlops")
mlops$init()
print("After mlops.init")

mlcomp <- import("parallelm.components.external_component")
mlcomp <- mlcomp$mlcomp
params = mlcomp$get_params()
print(paste0("exit_status:    ", params["exit_status"]))
print(paste0("expected_input: ", params["expected_input_str"]))
print(paste0("num_iter:    ", params["num_iter"]))
print(paste0("input_model: ", params["input_model"]))

if(!file.exists(params[["input_model"]])) {
    stop("error message - model file does not found")
}


parents_objs_list = mlcomp$get_parents_objs()
print(paste0("parents_objs_list lenght:", length(parents_objs_list)))
input_str = parents_objs_list[1]
print(paste0("str1: ", input_str))

if(params[["expected_input_str"]] != input_str) {
    stop("Input string is not as expected")
}

# Setting the output of this component to be a single string object
mlcomp$set_output_objs(input_str)

## MLOps example 1
mlops$set_stat("r-code-starting", 1)


# Code to generate a model

# Code to save the model

## MLOps done to stop the library
mlops$done()
