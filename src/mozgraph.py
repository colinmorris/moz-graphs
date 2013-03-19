__author__ = 'colin'

class MozGraph(object): # TODO: Should this inherit from igraph.Graph? Probably yes
    """A graph of bugs and debuggers during a particular month.
    """

    def __init__(self, session):
        # TODO: Should this load directly from a session? Or should we dump to a file using an adj-matrix format,
        # and then load from the file? Probably the latter is easier.
        # We could have a static method to load a mozgraph corresponding to a particular bugmonth object
        # (Just as a convenience so the calling context doesn't need to deal with parsing filenames and paths)
        raise NotImplementedError

    def get_vertex(self, obj):
        """Get the vertex corresponding to a particular object, which may be either a
        Bug or a Debugger.
        """
        raise NotImplementedError

class MozIRCGraph(MozGraph):
    """A graph of just debuggers during a particular month. The only links are
    debugger-debugger, i.e. IRC links. Most of our bugmonth variables only use
    the IRC network.
    """

    pass