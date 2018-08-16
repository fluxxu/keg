import struct
from binascii import hexlify
from io import BytesIO
from math import ceil
from typing import IO, Dict, Iterable, List, Tuple

from . import blte
from .utils import read_cstr, verify_data


class TagError(KeyError):
	pass


class InstallFile:
	def __init__(self, contents: bytes, key: str, verify: bool=False) -> None:
		self.key = key
		verify_data("install file", contents, key, verify)
		self.tags: Dict[str, Tuple[int, bytes]] = {}
		self.entries: List[Tuple[str, str, int]] = []
		self.parse_bytes(contents)

	@classmethod
	def from_blte_file(self, fp: IO, key: str, encoded_key: str, verify: bool=False):
		contents = blte.load(fp, encoded_key, verify=verify)
		return InstallFile(contents, key, verify=verify)

	def parse_bytes(self, data: bytes):
		contents = BytesIO(data)

		assert contents.read(2) == b"IN"

		version, hash_size, tag_count, entry_count = struct.unpack(
			">BBHI", contents.read(8)
		)

		for i in range(tag_count):
			tag_name = read_cstr(contents)
			type, = struct.unpack(">H", contents.read(2))
			data = contents.read(ceil(entry_count / 8))
			self.tags[tag_name] = (type, data)

		for i in range(entry_count):
			file_name = read_cstr(contents)
			digest = hexlify(contents.read(hash_size)).decode()
			size, = struct.unpack(">I", contents.read(4))
			self.entries.append((file_name, digest, size))

	def filter_entries(self, tags: List[str]) -> Iterable[Tuple[str, str, int]]:
		tag_arrays = []
		for tag in tags:
			if tag not in self.tags:
				raise TagError(tag)
			flags = self.tags[tag][1]
			tag_arrays.append(
				[flags[i // 8] & 1 << i % 8 != 0 for i in range(len(flags) * 8)]
			)

		for i, entry in enumerate(self.entries):
			if all(array[i] for array in tag_arrays):
				yield entry
