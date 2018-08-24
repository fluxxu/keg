from hashlib import md5
from io import BytesIO

import pytest

from keg import blte
from keg.exceptions import BLTEError

from . import get_resource


def test_verify_good_blte():
	key = "ffe7577ae7627e4c90bd4836f1b84479"
	with get_resource(f"blte/{key}", "rb") as fp:
		blte.verify_blte_data(fp, key)


def test_verify_blte_extra_data():
	key = "ffe7577ae7627e4c90bd4836f1b84479"
	with get_resource(f"blte/{key}", "rb") as fp:
		data = fp.read()

	fp = BytesIO(data + b"B")
	with pytest.raises(BLTEError):
		blte.verify_blte_data(fp, key)


def test_blte_encode():
	key = "2a6168d8a7122a8dd9b61fb92af3d3f4"
	spec = "b:{22=n,54=z,192=n,24576=n,128=n,16384=n,*=z}"
	with get_resource(f"blte/{key}.in", "rb") as fp:
		data = fp.read()

	data, written, out_key = blte.dumps(data, spec)
	assert key == out_key
	assert md5(data).hexdigest() == "39c6c6b7b1fecd09a1d6514470988700"
