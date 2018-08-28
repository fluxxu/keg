from keg.configfile import BuildConfig

from . import get_resource


def test_split_keys_two_install_keys():
	# install = 26310bf3c01df9a385813037e1710e50 229de3024448d226c7a35bbb8fefb046
	with get_resource("buildconfig/f7e68fd6611317050be908301b944855", "rb") as f:
		bc = BuildConfig.from_bytes(f.read())

	assert bc.install.content_key == "26310bf3c01df9a385813037e1710e50"
	assert bc.install.encoding_key == "229de3024448d226c7a35bbb8fefb046"


def test_split_keys_one_install_key():
	# install = b0c59af62001174f3d0857d07e8784c2
	with get_resource("buildconfig/6a5f9d058ac7c519d929571a64e4ef3d", "rb") as f:
		bc = BuildConfig.from_bytes(f.read())

	assert bc.install.content_key == "b0c59af62001174f3d0857d07e8784c2"
	assert bc.install.encoding_key == ""
