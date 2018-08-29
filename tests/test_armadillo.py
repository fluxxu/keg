from base64 import b32decode
from hashlib import md5

from keg.armadillo import ARMADILLO_KEY_SIZE, ArmadilloKey, verify_armadillo_key

from . import get_resource


FULL_KEY = b32decode("6Z45YOHAYNS7WSBOJCTUREE5FEM7LO4I")
AK = ArmadilloKey(FULL_KEY[:ARMADILLO_KEY_SIZE])


def test_verify_armadillo_key():
	assert verify_armadillo_key(FULL_KEY)


def test_decrypt_buildconfig():
	key = "e32f46c7245bfc154e43924555a5cf9f"

	with get_resource(f"buildconfig/encrypted/{key}", "rb") as f:
		encrypted_data = f.read()

	decrypted_data = AK.decrypt_object(key, encrypted_data)
	assert md5(decrypted_data).hexdigest() == key
