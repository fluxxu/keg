from keg.cdn import RemoteCDN


def test_remote_path_join():
	cdn = RemoteCDN("http://example.com", "/test/path", "/test/config-path")
	assert cdn._join_path("/path", "foo/") == "/path/foo/"
	assert cdn._join_path("/path/", "foo/") == "/path/foo/"
	assert cdn._join_path("/path/", "/foo/") == "/path/foo/"
	assert cdn._join_path("path/", "/foo/") == "path/foo/"
	assert cdn._join_path("path", "/foo/") == "path/foo/"
