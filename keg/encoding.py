import struct
from binascii import hexlify
from io import BytesIO
from typing import Iterable


class EncodingFile:
	def __init__(self, data: bytes) -> None:
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
