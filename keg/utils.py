import hashlib
import os

from .exceptions import IntegrityVerificationError


def atomic_write(path: str, content: bytes) -> int:
	temp_path = path + ".keg_temp"
	with open(temp_path, "wb") as f:
		ret = f.write(content)
	os.rename(temp_path, path)
	return ret


def partition_hash(hash: str) -> str:
	if len(hash) < 4:
		raise ValueError(f"Invalid hash to partition: {repr(hash)}")
	return f"{hash[0:2]}/{hash[2:4]}/{hash}"


def verify_data(object_name: str, data: bytes, key: str, verify: bool) -> bool:
	if verify:
		digest = hashlib.md5(data).hexdigest()
		if digest != key:
			raise IntegrityVerificationError(object_name, digest, key)

	return True
