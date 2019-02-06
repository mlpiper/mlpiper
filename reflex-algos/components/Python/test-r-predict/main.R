#!/usr/bin/env Rscript

# Required for reticulate to work properly
Sys.setenv(MKL_THREADING_LAYER = "GNU")

library("reticulate")
library("optparse")

Sys.getenv(c("PYTHONPATH"))
py_config()

mlops <- import("parallelm.mlops", convert = TRUE)
mlops <- mlops$mlops
print("After import")
mlops$init()
print("After mlops.init")

option_list = list(make_option(c("--data-file"), type="character", default=NULL,
                                 help="dataset file name", metavar="character"),
                   make_option(c("--input-model"), type="character", default=NULL,
                                help="input model to use for predictions", metavar="character")
);

opt_parser = OptionParser(option_list=option_list);
opt = parse_args(opt_parser, convert_hyphens_to_underscores=TRUE);

if (is.null(opt$data_file)){
    print_help(opt_parser)
    stop("At least one argument must be supplied (input file).n", call.=FALSE)
}

if (is.null(opt$input_model)){
    print_help(opt_parser)
    stop("At least one argument must be supplied (input model).n", call.=FALSE)
}

print("test-r-predict", quote = FALSE)
print(paste0("data_file:   ", opt$data_file))
print(paste0("input_model: ", opt$input_model))


mlops$set_stat("r-code-starting", 1)


## a month later, new observations are available:
newdf <- data.frame(x = rnorm(20))

## MLOps done to stop the library
mlops$done()
