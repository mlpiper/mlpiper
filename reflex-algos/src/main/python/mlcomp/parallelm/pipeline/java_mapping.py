
MODEL_FILE_SOURCE_PATH_KEY = 'modelFileSourcePath'

TAGS = {'model_dir': 'modelFileSinkPath',
        'input_model_path': MODEL_FILE_SOURCE_PATH_KEY}

RESERVED_KEYS = {k: "__{}__tag__".format(k) for (k, v) in TAGS.items()}
