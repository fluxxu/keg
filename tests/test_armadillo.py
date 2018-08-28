from base64 import b32decode

from keg.armadillo import verify_armadillo_key


def test_verify_armadillo_key():
	assert verify_armadillo_key(b32decode("6Z45YOHAYNS7WSBOJCTUREE5FEM7LO4I"))
