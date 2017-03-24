
import pytest
def pytest_addoption(parser):
    parser.addoption("--long", action="store_true",
        help="run long tests")