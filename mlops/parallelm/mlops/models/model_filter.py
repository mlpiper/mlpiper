class ModelFilter(object):
    def __init__(self):
        self.id = None
        self.time_window_start = None
        self.time_window_end = None

    def __str__(self):
        return "id: {} time_window[{}, {}]".format(self.id, self.time_window_start,
                                                   self.time_window_end)

