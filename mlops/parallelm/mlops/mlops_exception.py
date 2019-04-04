import functools
import logging


class MLOpsException(Exception):
    """
    Exception that will be returned by the MLOps class on error
    """
    pass


class MLOpsConnectionException(MLOpsException):
    """
    Exception that will be returned by the MLOps class on connection error
    """
    pass


"""
SuppressException decorator can be used to suppress Exceptions.

When used without arguments, by default all the Exceptions will be suppressed.
@SuppressException()
method()

If used with arguments:
All exceptions inherited from GenericException will be suppressed.
@SuppressException([GenericException])
method()

TODO: currently the only usage is to suppress ConnectionException.
When decorator will be used to suppress several types of exceptions,
and it will be needed to suppress A but not suppress B, include/exclude lists/APIs can be added.

e.g
@SuppressException([A, C])
method1()

@SuppressException([B])
method2()

SuppressException.set_suppress(True)  # all A, B, C will be suppressed
SuppressException.include([A, B]) # only A, B will be suppressed
SuppressException.exclude([B]) # only A will be suppressed

+ maybe add return value for suppression definition.
"""
    
    
class SuppressException(object):
    _suppress = False

    def __init__(self, exc_list=[Exception], handler=None):
        self._logger = logging.getLogger(SuppressException.__name__)
        self._handler = self._warn if handler is None else handler
        self._exceptions = tuple(exc_list)

    def _warn(self, e):
        self._logger.warning("suppressed exception: {}".format(str(e)))

    def __call__(self, func):
        """
        If there are decorator arguments, __call__() is only called
        once, as part of the decoration process! You can only give
        it a single argument, which is the function object.
        """

        # The meaning of decorator is that wrapper
        # wraps decorated func, and replaces it.
        # So when one calls func() actually wrapper() will be called.
        # `that` is `self` from a decorated method
        @functools.wraps(func)
        def wrapper(that, *args, **kwargs):
            if not self._suppress:
                return func(that, *args, **kwargs)
            else:
                try:
                    return func(that, *args, **kwargs)
                except self._exceptions as e:
                    self._handler(e)
                return None

        return wrapper

    @classmethod
    def set_suppress(cls, suppress):
        """
        Turn on/off exception suppression for methods decorated by :class:`SuppressException`

        :param suppress: boolean True/False
        """
        cls._suppress = suppress
