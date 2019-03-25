import pytest

from parallelm.mlops.mlops_exception import MLOpsException, SuppressException
from parallelm.mlops import mlops


class MLOpsTestException(MLOpsException):
    pass


exception_handler_called = False


def exception_handler(e):
    global exception_handler_called
    exception_handler_called = type(e) == MLOpsTestException


class TestApi(object):

    @SuppressException()
    def api_raise_exception(self):
        raise Exception("raising exception")

    @SuppressException([MLOpsException])
    def api_raise_mlops_exception(self):
        raise MLOpsException("raising MLOps exception")

    @SuppressException([MLOpsTestException])
    def api_raise_mlops_test_exception(self):
        raise MLOpsTestException("raising MLOps test exception")

    @SuppressException([MLOpsTestException])
    def api_raise_mlops_or_test_exception(self, arg=0):
        if arg == 0:
            raise MLOpsException("raising MLOps exception")
        else:
            raise MLOpsTestException("raising MLOps test exception")

    @SuppressException([MLOpsTestException], exception_handler)
    def api_raise_mlops_test_exception2(self):
        raise MLOpsTestException("raising MLOps test exception")


def test_not_suppressed_exceptions():
    ta = TestApi()
    mlops.suppress_connection_errors(False)
    with pytest.raises(Exception):
        ta.api_raise_exception()

    with pytest.raises(MLOpsException):
        ta.api_raise_mlops_exception()

    with pytest.raises(MLOpsTestException):
        ta.api_raise_mlops_test_exception()

    with pytest.raises(MLOpsException):
        ta.api_raise_mlops_or_test_exception()

    with pytest.raises(MLOpsTestException):
        ta.api_raise_mlops_or_test_exception(1)

    with pytest.raises(MLOpsTestException):
        ta.api_raise_mlops_test_exception2()


def test_suppressed_exceptions():

    ta = TestApi()
    mlops.suppress_connection_errors(True)

    # Exception class is suppressed
    ta.api_raise_exception()

    # MLOpsException and MLOpsTestException are suppressed
    ta.api_raise_mlops_exception()
    ta.api_raise_mlops_test_exception()

    # MLOpsTestException is suppressed...
    ta.api_raise_mlops_or_test_exception(1)

    # ...but not MLOpsException
    with pytest.raises(MLOpsException):
        ta.api_raise_mlops_or_test_exception(0)

    # test MLOpsTestException is suppressed and user provided handler is called
    ta.api_raise_mlops_test_exception2()
    assert(exception_handler_called)

    # turn suppression off
    mlops.suppress_connection_errors(False)
    with pytest.raises(MLOpsTestException):
        ta.api_raise_mlops_test_exception()
