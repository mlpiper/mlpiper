
MODEL_FILE_SINK_PATH_KEY = 'modelFileSinkPath'
MODEL_FILE_SOURCE_PATH_KEY = 'modelFileSourcePath'

TAGS = {'model_dir': MODEL_FILE_SINK_PATH_KEY,
        'input_model_path': MODEL_FILE_SOURCE_PATH_KEY}

RESERVED_KEYS = {k: "__{}__tag__".format(k) for (k, v) in TAGS.items()}
