import os


def _get_resource(path: str):
	return open(os.path.join(os.path.dirname(__file__), "res", path))


def test_read_psv():
	from keg import psv

	with _get_resource("versions.psv") as fp:
		data = psv.load(fp)

	assert data
