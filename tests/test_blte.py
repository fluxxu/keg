
from io import BytesIO

import pytest

from keg.blte import verify_blte_data
from keg.exceptions import BLTEError

from . import get_resource


def test_verify_good_blte():
	key = "ffe7577ae7627e4c90bd4836f1b84479"
	with get_resource(f"blte/{key}", "rb") as fp:
		verify_blte_data(fp, key)


def test_verify_blte_extra_data():
	key = "ffe7577ae7627e4c90bd4836f1b84479"
	with get_resource(f"blte/{key}", "rb") as fp:
		data = fp.read()

	fp = BytesIO(data + b"B")
	with pytest.raises(BLTEError):
		verify_blte_data(fp, key)
