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
print(paste0("num_iter:   ", params["num_iter"]))
print(paste0("model_file: ", params["output_model"]))

parents_objs_list = mlcomp$get_parents_objs()
print(paste0("parents_objs_list lenght:", length(parents_objs_list)))
df1 = parents_objs_list[1]
str1 = parents_objs_list[2]

print(df1)
print(paste0("str1: ", str1))

test = data.frame(temperature_1 = rnorm(100),
                  temperature_2 = rnorm(100),
                  pressure_1 = sample.int(10, 100, replace = TRUE),
                  pressure_2 = rep(c("A"    , "B", "C", "D"), 5))

print("Reporting L2 stats")
mlops$set_data_distribution_stat(data=test)

mlcomp$set_output_objs("s3:from-within-r-code")

## MLOps example 1
mlops$set_stat("r-code-starting", 1)


df <- data.frame(x = rnorm(20))
df <- transform(df, y = 5 + (2.3 * x) + rnorm(20))
m1 <- lm(y ~ x, data = df)
## save this model

saveRDS(m1, file =  params[["output_model"]])

## MLOps done to stop the library
mlops$done()
