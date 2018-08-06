import struct
from binascii import hexlify
from io import BytesIO
from os import SEEK_CUR, SEEK_END
from typing import Iterable, List


class Archive:
	def __init__(self, key: str) -> None:
		self.key = key

	def __repr__(self):
		return f"<{self.__class__.__name__}: {self.key}>"


class ArchiveIndex:
	def __init__(self, data: bytes, key: str, verify: bool=False) -> None:
		self.key = key
		self.verify = verify

		self.data = BytesIO(data)
		self.data.seek(-28, SEEK_END)

		(
			toc_hash,
			version,
			_,
			_,
			self.block_size_kb,
			self.offset_size,
			self.size_size,
			self.key_size,
			checksum_size,
			self.num_items,
			footer_checksum
		) = struct.unpack("<8s8BI8s", self.data.read())

	def __repr__(self):
		return f"<{self.__class__.__name__}: {self.key}>"

	@property
	def items(self):
		self.data.seek(0)

		bytes_left_in_block = self.block_size_kb * 1024

		for i in range(self.num_items):
			bytes_to_read = self.key_size + self.size_size + self.offset_size
			if bytes_to_read > bytes_left_in_block:
				self.data.seek(bytes_left_in_block, SEEK_CUR)
				bytes_left_in_block = self.block_size_kb * 1024
			bytes_left_in_block -= bytes_to_read

			_data = self.data.read(bytes_to_read)
			key, size, offset = struct.unpack(">16sII", _data)
			key = hexlify(key).decode()
			yield key, size, offset


class ArchiveGroupIndex:
	def __init__(
		self, archive_indices: Iterable[ArchiveIndex], key: str, verify: bool=False
	) -> None:
		# sort keys in all indices and write them to blocks
		# write toc from last hash in blocks and md5sum(block_data)
		# write footer from md5sum(toc) and with md5sum(footer)
		# verify archive name against md5sum(toc_hash + footer)

		self.archive_indices = archive_indices
		self.key = key
		self.items = sorted(
			(key, size, archive_id, offset)
			for archive_id, archive_index in enumerate(self.archive_indices)
			for key, size, offset in archive_index.items
		)

		# TODO: write to disk /impl write method, then verify

	def __repr__(self):
		return f"<{self.__class__.__name__}: {self.key}>"


class ArchiveGroup:
	def __init__(self, archive_keys: List[str], key: str, verify: bool=False) -> None:
		self.archive_keys = archive_keys
		self.key = key
		self.verify = verify

		self.archives: List[Archive] = [
			Archive(archive_key) for archive_key in archive_keys
		]

	def __repr__(self):
		return f"<{self.__class__.__name__}: {self.key}>"

	@property
	def archives(self) -> Iterable[Archive]:
		for archive_key in self.archive_keys:
			yield Archive(archive_key)

	def get_indices(self, cdn) -> Iterable[ArchiveIndex]:
		for archive_key in self.archive_keys:
			yield cdn.download_data_index(archive_key, verify=self.verify)

	def get_merged_index(self, cdn) -> ArchiveGroupIndex:
		return ArchiveGroupIndex(
			self.get_indices(cdn), self.key, verify=self.verify
		)
