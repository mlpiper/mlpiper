class GeneralHistogramDataObject(object):

    def __init__(self, feature_name, edges, bins):
        self._feature_name = feature_name
        self._edges = edges
        self._bins = bins

    def get_feature_name(self):
        return self._feature_name

    def get_edges(self):
        """String representation of Histogram"""
        return self._edges

    def get_bins(self):
        return self._bins

    def get_edge_list(self):
        """
        String representation of Histogram
        Child can override this method.
        """

        return self.get_edges()

    def __str__(self):
        return "\nf_name: {}\nedges: {}\nbins: {}".format(str(self._feature_name), str(self._edges), str(self._bins))


class CategoricalHistogramDataObject(GeneralHistogramDataObject):
    """
    Class is responsible for holding categorical histogram representation.
    """

    def __init__(self, feature_name, edges, bins):
        self._feature_name = feature_name
        self._edges = edges
        self._bins = bins

        super(CategoricalHistogramDataObject, self).__init__(feature_name=feature_name, edges=edges, bins=bins)

    def get_feature_name(self):
        return self._feature_name

    def get_edges(self):
        return self._edges

    def get_bins(self):
        return self._bins


class ContinuousHistogramDataObject(GeneralHistogramDataObject):
    """
    Class is responsible for holding continuous histogram representation.
    """

    def __init__(self, feature_name, edges, bins):
        self._feature_name = feature_name
        self._edges = edges
        self._bins = bins

        super(ContinuousHistogramDataObject, self).__init__(feature_name=feature_name, edges=edges, bins=bins)

    def get_feature_name(self):
        return self._feature_name

    def get_edges(self):
        return self._edges

    def get_bins(self):
        return self._bins

    def get_edge_list(self):
        """Integer representation of Continuous Histogram"""

        edge_list = []

        for each_edge_tuple in self.get_edges():
            edge_t = each_edge_tuple.split("to")
            edge_list.append(float(edge_t[0]))
        edge_list.append(float(self.get_edges()[len(self.get_edges()) - 1].split("to")[1]))
        return edge_list
