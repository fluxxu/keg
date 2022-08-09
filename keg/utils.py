import hashlib
import os
from io import IOBase
from typing import IO, AnyStr

from .exceptions import IntegrityVerificationError


class TqdmReadable(IOBase):
	"""Wraps an underlying IO object to instrument calls to read() through a tqdm bar."""

	def __init__(self, readable: IO, bar):
		self.readable = readable
		self.bar = bar

	def __enter__(self):
		return self

	def __exit__(self, exc_type, exc_val, exc_tb):
		self.readable.close()
		self.bar.close()

	def read(self, size: int = -1) -> AnyStr:
		ret = self.readable.read(size)
		if ret:
			self.bar.update(len(ret))
		return ret


def atomic_write(path: str, content: bytes) -> int:
	temp_path = path + ".keg_temp"
	with open(temp_path, "wb") as f:
		ret = f.write(content)
	os.rename(temp_path, path)
	return ret


def ensure_dir_exists(path: str) -> None:
	dirname = os.path.dirname(path)
	if not os.path.exists(dirname):
		os.makedirs(dirname)


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


def read_cstr(fp: IO) -> str:
	ret = []

	while True:
		c = fp.read(1)
		if not c or c == b"\0":
			break
		ret.append(c)

	return b"".join(ret).decode()
