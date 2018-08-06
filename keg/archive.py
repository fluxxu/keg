import struct
from binascii import hexlify
from io import BytesIO
from os import SEEK_CUR, SEEK_END


class Archive:
	def __init__(self, key: str, verify: bool=False) -> None:
		self.key = key
		self.verify = verify

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


class ArchiveGroup:
	pass
