import struct
import zlib
from binascii import hexlify
from hashlib import md5
from io import BytesIO
from typing import IO, Iterable, List, Tuple

from . import espec
from .exceptions import BLTEError
from .utils import verify_data


# 00000000: 424c 5445 0000 00b4 0f00 0007 0000 0017  BLTE............
#           ^ 4-byte BLTE magic
#                     ^ 4-byte big-endian header size
#                               ^ 1-byte version
#                                 ^ 3-byte (big-endian) number of blocks
#                                         ^ first block encoded size
# 00000010: 0000 0016 6f65 6f53 d85a d828 108e 8444  ....oeoS.Z.(...D
#           ^ first block decoded size
#                     ^ first block encoded md5 hash
# 00000020: 8535 b202 0000 003b 0000 0036 f490 b704  .5.....;...6....
#                     ^ second block encoded size ...
# 00000030: 25e9 4f52 047f 7583 2f64 3b40 0000 0101  %.OR..u./d;@....
# 00000040: 0000 0100 35e0 1068 8ac0 e7d9 4a67 6e89  ....5..h....Jgn.
# 00000050: 72ed 60c2 0000 8001 0000 8000 7d54 b708  r.`.........}T..
# 00000060: 1246 2e1a 377b d2ba 578e 6d35 0000 00a1  .F..7{..W.m5....
# 00000070: 0000 00a0 4678 ee15 76ba 8650 5c6a 29fa  ....Fx..v..P\j).
# 00000080: 009c a687 0000 5001 0000 5000 2632 c86f  ......P...P.&2.o
# 00000090: 7bc2 cbfc 89f4 e93a f8ef c120 0000 002f  {......:... .../
# 000000a0: 0000 002d 0f44 12c6 9a86 82cf 1bfe 63d5  ...-.D........c.
# 000000b0: 8564 1952 4e45 4e01 1010 0004 0004 0000  .d.RNEN.........
#                     ^ first block data
# 000000c0: 0008 0000 0005 0000 0000 365a 78da 4bb2  ..........6Zx.K.
#                                      ^ second block data


def decode_block(data: bytes) -> bytes:
	type = data[0]

	if type == b"N"[0]:
		return data[1:]
	elif type == b"Z"[0]:
		return zlib.decompress(data[1:], wbits=0)

	raise ValueError(f"Unknown block type {type}")


def verify_blte_data(fp: IO, key: str):
	dec = BLTEDecoder(fp, key, verify=True)
	for block in dec.encoded_blocks:
		# Iterating verifies the block
		pass

	if dec.header_size:
		# Single-frame BLTEs are already verified in .encoded_blocks
		# For normal BLTEs, we need to check that there isn't more data at the end.
		pos = dec.fp.tell()
		dec.fp.read()
		pos2 = dec.fp.tell()
		if pos != pos2:
			raise BLTEError(f"{pos2 - pos} extra bytes at the end of the file")


class BLTEDecoder:
	def __init__(self, fp: IO, key: str, verify: bool=False) -> None:
		self.fp = fp
		self.block_table: List[Tuple[int, int, str]] = []
		self._block_index = 0
		self.key = key
		self.verify = verify
		self.parse_header()

	def parse_header(self):
		self._header_data = self.fp.read(8)
		blte_header = BytesIO(self._header_data)
		assert blte_header.read(4) == b"BLTE"
		self.header_size, = struct.unpack(">i", blte_header.read(4))

		if self.header_size > 0:
			assert self.fp.read(1) == b"\x0f"
			block_info_data = self.fp.read(self.header_size - 9)
			if self.verify:
				_data_to_verify = self._header_data + b"\x0f" + block_info_data
				verify_data("BLTE header", _data_to_verify, self.key, self.verify)

			block_info = BytesIO(block_info_data)
			self.parse_block_info(block_info)

	def parse_block_info(self, fp: IO) -> None:
		num_blocks, = struct.unpack(">i", b"\x00" + fp.read(3))
		for i in range(num_blocks):
			encoded_size, decoded_size, digest = struct.unpack(
				">ii16s", fp.read(4 + 4 + 16)
			)
			self.block_table.append(
				(encoded_size, decoded_size, hexlify(digest).decode())
			)

	@property
	def blocks(self) -> Iterable[bytes]:
		for encoded_block in self.encoded_blocks:
			yield decode_block(encoded_block)

	@property
	def encoded_blocks(self) -> Iterable[bytes]:
		if self._block_index:
			raise RuntimeError(
				"BLTE.blocks has already been iterated over. "
				"You should have stored it. "
				"Now you can't get it back."
			)

		if not self.block_table:
			data = self.fp.read()
			self._block_index += 1
			verify_data("single-frame BLTE", self._header_data + data, self.key, self.verify)
			yield data
			return

		for encoded_size, decoded_size, digest in self.block_table:
			data = self.fp.read(encoded_size)
			verify_data("BLTE block", data, digest, self.verify)
			self._block_index += 1
			yield data

	def decode_and_write(self, fp: IO) -> int:
		"""
		Writes the decoded content of the BLTE file to the given file-like object.
		"""
		ret = 0
		for block in self.blocks:
			ret += fp.write(block)
		return ret


def load(fp: IO, key: str, verify: bool=False) -> bytes:
	decoder = BLTEDecoder(fp, key, verify=verify)
	return b"".join(decoder.blocks)


def loads(data: bytes, key: str, verify: bool=False) -> bytes:
	fp = BytesIO(data)
	return load(fp, key, verify=verify)


class BLTEEncoder:
	def __init__(self, spec: espec.EncodingSpec) -> None:
		self.spec = spec

	def write(self, data: IO, fp: IO) -> Tuple[int, str]:
		encoded_data = BytesIO()
		num_blocks = 0
		block_table = []

		if isinstance(self.spec.frame, espec.BlockTableFrame):
			for block_size, repeat, subframe in self.spec.frame.frame_info:
				if isinstance(subframe, espec.RawFrame):
					callback = lambda d: b"N" + d  # noqa
				elif isinstance(subframe, espec.ZipFrame):
					def callback(d: bytes) -> bytes:
						compressor = zlib.compressobj(level=subframe.level, wbits=subframe.bits)
						return b"Z" + compressor.compress(d) + compressor.flush()
				else:
					raise NotImplementedError

				if repeat == -1:
					while True:
						data_block = data.read(block_size)
						if not data_block:
							break
						num_blocks += 1
						encoded_block = callback(data_block)
						digest = md5(encoded_block).digest()
						encoded_data.write(encoded_block)
						block_table.append((len(encoded_block), len(data_block), digest))
				else:
					for i in range(repeat):
						num_blocks += 1
						data_block = data.read(block_size)
						encoded_block = callback(data_block)
						digest = md5(encoded_block).digest()
						encoded_data.write(encoded_block)
						block_table.append((len(encoded_block), len(data_block), digest))
		else:
			raise NotImplementedError

		header = BytesIO()
		header.write(b"BLTE")
		subheader = BytesIO()
		subheader.write(b"\x0f")
		subheader.write(struct.pack(">i", num_blocks)[1:])
		import tabulate
		print(tabulate.tabulate((k[0], k[1], hexlify(k[2])) for k in block_table))
		for block_info in block_table:
			subheader.write(struct.pack(">ii16s", *block_info))

		header_size = len(subheader.getvalue()) + 8
		header.write(struct.pack(">i", header_size))
		header.write(subheader.getvalue())

		header_data = header.getvalue()
		blte_key = md5(header_data).hexdigest()
		return fp.write(header_data + encoded_data.getvalue()), blte_key


def dump(data: bytes, fp: IO, spec: espec.EncodingSpec) -> Tuple[int, str]:
	if isinstance(spec, str):
		spec = espec.EncodingSpec(spec)
	encoder = BLTEEncoder(spec)
	written, key = encoder.write(BytesIO(data), fp)
	return written, key
