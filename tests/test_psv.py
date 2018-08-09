import os


def _get_resource(path: str):
	return open(os.path.join(os.path.dirname(__file__), "res", path))


def test_read_psv():
	from keg import psv

	with _get_resource("versions.psv") as fp:
		data = psv.load(fp)

	assert data.header == [
		"Region",
		"BuildConfig",
		"CDNConfig",
		"KeyRing",
		"BuildId",
		"VersionsName",
		"ProductConfig",
	]
	assert len(data.rows) == 7
	assert list(data.rows[0]) == [
		"us",
		"4eb3986466ec004ffa1755642b375a87",
		"fb445ca0526699c61a92830ab894a985",
		"",
		"27291",
		"8.0.1.27291",
		"19a26886b5b1c264de1177ae6aa7fbf5",
	]
