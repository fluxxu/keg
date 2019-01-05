import os
from typing import IO


def get_resource(path: str, mode="r") -> IO:
	return open(os.path.join(os.path.dirname(__file__), "res", path), mode)
