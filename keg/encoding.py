import struct
from binascii import hexlify
from io import BytesIO
from typing import IO, Dict, Iterable, List, Tuple, Union

from . import blte
from .utils import verify_data


class EncodingFile:
	def __init__(
		self, data: Union[IO, bytes], content_key: str, encoded_key: str, verify: bool=False
	) -> None:
		self.content_key = content_key
		self.encoded_key = encoded_key

		self._encoding_keys: List[Tuple[str, str]] = []
		self._content_keys: Dict[str, List[str]] = {}
		if isinstance(data, bytes):
			decoded_data = blte.loads(data, encoded_key, verify=verify)
		else:
			decoded_data = blte.load(data, encoded_key, verify=verify)

		verify_data("encoding file", decoded_data, content_key, verify)
		self.parse_header(decoded_data)

	def __repr__(self) -> str:
		return f"<{self.__class__.__name__}: {self.content_key} {self.encoded_key}>"

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
	def encoding_keys(self) -> Iterable[Tuple[str, str]]:
		if self._encoding_keys:
			yield from self._encoding_keys
			return

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
				key = hexlify(page[ofs:ofs + self.encoding_hash_size]).decode()
				self._encoding_keys.append(key)
				yield key, self.specs[espec_index]
				ofs += self.encoding_hash_size + 9

	@property
	def content_keys(self) -> Iterable[Tuple[str, List[str]]]:
		if self._content_keys:
			yield from self._content_keys.items()
			return

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

				self._content_keys[content_key] = keys
				yield content_key, keys

	def preload_content(self) -> None:
		if not self._content_keys:
			# Fill content key cache by iterating without doing anything
			for obj in self.content_keys:
				pass
			assert self._content_keys

	def preload_encoding(self) -> None:
		if not self._encoding_keys:
			# Fill encoding key cache by iterating without doing anything
			for obj in self.encoding_keys:
				pass
			assert self._encoding_keys

	def has_encoding_key(self, key: str) -> bool:
		self.preload_encoding()
		return key in self._encoding_keys

	def find_by_content_key(self, key: str) -> str:
		self.preload_content()
		return self._content_keys[key][0]
