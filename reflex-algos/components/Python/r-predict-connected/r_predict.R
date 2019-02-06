#!/usr/bin/env Rscript

# Required for reticulate to work properly
Sys.setenv(MKL_THREADING_LAYER = "GNU")

library("reticulate")

mlops <- import("parallelm.mlops", convert = TRUE)
mlops <- mlops$mlops
print("After import of mlops")
mlops$init()
print("After mlops.init")

mlcomp <- import("parallelm.components.external_component")
mlcomp <- mlcomp$mlcomp
params = mlcomp$get_params()
print(paste0("num_iter:    ", params["num_iter"]))
print(paste0("input_model: ", params["input_model"]))

parents_objs_list = mlcomp$get_parents_objs()
print(paste0("parents_objs_list lenght:", length(parents_objs_list)))
df1 = parents_objs_list[1]
str1 = parents_objs_list[2]

print(df1)
print(paste0("str1: ", str1))

# Generating a dummy dataset to demo L2 statistics
test = data.frame(temperature_1 = rnorm(100),
                  temperature_2 = rnorm(100),
                  pressure_1 = sample.int(10, 100, replace = TRUE),
                  pressure_2 = rep(c("A"    , "B", "C", "D"), 5))

print("Reporting L2 stats")
mlops$set_data_distribution_stat(data=test)


# Setting the output of this component to be a single string object
mlcomp$set_output_objs("s1", "s2")

## MLOps example 1
mlops$set_stat("r-code-starting", 1)


# Code to generate a model

# Code to save the model

## MLOps done to stop the library
mlops$done()
