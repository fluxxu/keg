from base64 import b32encode
from binascii import hexlify, unhexlify
from hashlib import md5

from Crypto.Cipher import Salsa20  # type: ignore

from .exceptions import IntegrityVerificationError


ARMADILLO_KEY_SIZE = 16
ARMADILLO_DIGEST_SIZE = 4


def verify_armadillo_key(data: bytes) -> bool:
	"""
	Verifies an Armadillo Key against itself.

	The expected data is a 16 byte key followed by an 8 byte digest.
	The digest is the first 8 bytes of the md5 of the key.
	"""
	if len(data) != ARMADILLO_KEY_SIZE + ARMADILLO_DIGEST_SIZE:
		raise ValueError(f"Invalid Armadillo Key size.")

	actual_data = data[:ARMADILLO_KEY_SIZE]
	expected_digest = hexlify(data[ARMADILLO_KEY_SIZE:]).decode()
	actual_digest = hexlify(md5(actual_data).digest()[:ARMADILLO_DIGEST_SIZE]).decode()
	digest = actual_digest

	if digest != expected_digest:
		raise IntegrityVerificationError("armadillo key", digest, expected_digest)

	return True


class ArmadilloKey:
	def __init__(self, data: bytes) -> None:
		self.data = data
		self.key = data[:ARMADILLO_KEY_SIZE]

	def __repr__(self):
		return f"<{self.__class__.__name__}: {b32encode(self.data)}>"

	def decrypt_object(self, key: str, data: bytes) -> bytes:
		nonce = unhexlify(key)[-8:]
		cipher = Salsa20.new(key=self.key, nonce=nonce)
		return cipher.decrypt(data)
