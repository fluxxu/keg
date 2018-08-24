from keg.encoding import EncodingFile

from . import get_resource


def test_encoding_file():
	key = "0839b3317e50fc5f8da4c6a30a2d1162"
	content_row_1 = (
		"06547b4248ca2559d515b925e0f9b59a", ["dca2fc45515fef35a293248f53648774"]
	)
	encoding_row_1 = ("0b71077b578e2108b42093632a2c5669", "z")

	with get_resource(f"encoding/{key}", "rb") as f:
		encoding = EncodingFile(f, "16f5c65b940fffcb94d175188b6751d2", key, verify=True)
		assert next(encoding.content_keys) == content_row_1
		assert next(encoding.encoding_keys) == encoding_row_1

		assert encoding.find_by_content_key(content_row_1[0]) == content_row_1[1][0]
		assert encoding.has_encoding_key(encoding_row_1[0])

		encoding.preload_content()
		encoding.preload_encoding()

	assert next(encoding.content_keys) == content_row_1
	assert next(encoding.encoding_keys) == encoding_row_1
