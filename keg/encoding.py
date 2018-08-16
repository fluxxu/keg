import struct
from binascii import hexlify
from io import BytesIO
from typing import Iterable, List, Tuple

from .utils import verify_data


class EncodingFile:
	def __init__(self, data: bytes, key: str, verify: bool=False) -> None:
		verify_data("encoding file", data, key, verify)
		self.parse_header(data)

	def parse_header(self, data: bytes) -> None:
		header_size = 22
		header = BytesIO(data[:header_size])

		assert header.read(2) == b"EN"
		assert header.read(1) == b"\1"

		(
			self.content_hash_size,
			self.encoding_hash_size,
			self.content_page_table_page_size,
			self.encoding_page_table_page_size,
			self.content_page_table_page_count,
			self.encoding_page_table_page_count,
			_,
			self.encoding_spec_block_size,
		) = struct.unpack(
			">BBHHIIBI", header.read(header_size - 3)
		)

		tmp_buffer = BytesIO(data[header_size:])
		spec_data = tmp_buffer.read(self.encoding_spec_block_size)
		self.specs = [spec.decode() for spec in spec_data.split(b"\0") if spec]

		self.content_page_table_index = BytesIO(tmp_buffer.read(
			self.content_page_table_page_count * (self.content_hash_size * 2)
		))
		self.content_page_table = BytesIO(tmp_buffer.read(
			self.content_page_table_page_count * 1024 * self.content_page_table_page_size
		))

		self.encoding_page_table_index = BytesIO(tmp_buffer.read(
			self.encoding_page_table_page_count * (self.encoding_hash_size * 2)
		))
		self.encoding_page_table = BytesIO(tmp_buffer.read(
			self.encoding_page_table_page_count * 1024 * self.encoding_page_table_page_size
		))

	@property
	def encoding_keys(self) -> Iterable[str]:
		self.encoding_page_table.seek(0)
		page_size = 1024 * self.encoding_page_table_page_size
		for i in range(self.encoding_page_table_page_count):
			ofs = 0
			page = self.encoding_page_table.read(page_size)
			while ofs + self.encoding_hash_size + 9 < page_size:
				espec_index, = struct.unpack(">i", page[
					ofs + self.encoding_hash_size:ofs + self.encoding_hash_size + 4
				])
				if espec_index == -1:
					break
				yield hexlify(page[ofs:ofs + self.encoding_hash_size]).decode()
				ofs += self.encoding_hash_size + 9

	@property
	def content_keys(self) -> Iterable[Tuple[str, List[str]]]:
		self.content_page_table.seek(0)
		page_size = 1024 * self.content_page_table_page_size
		for i in range(self.content_page_table_page_count):
			ofs = 0
			page = self.content_page_table.read(page_size)

			while ofs + 6 + self.content_hash_size + self.encoding_hash_size <= page_size:
				key_count, file_size_hi, file_size = struct.unpack(">BBI", page[
					ofs:ofs + 6
				])
				ofs += 6
				file_size |= file_size_hi << 32
				content_key = hexlify(page[ofs:ofs + self.content_hash_size]).decode()
				if not key_count:
					break
				ofs += self.content_hash_size
				keys = []
				for i in range(key_count):
					keys.append(hexlify(page[ofs:ofs + self.encoding_hash_size]).decode())
					ofs += self.encoding_hash_size

				yield content_key, keys

	def find_by_content_key(self, key) -> str:
		return dict(self.content_keys)[key][0]
