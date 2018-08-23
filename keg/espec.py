from typing import Type

from parsimonious.grammar import Grammar


GRAMMAR = Grammar("""
espec = data_raw / data_zipped / data_encrypted / data_block

data_raw = flag_raw
data_zipped = flag_zip (COLON zip_args)?
data_encrypted = flag_encrypted COLON encryption_args
data_block = flag_block COLON block_args

flag_raw = "n"
flag_zip = "z"
flag_encrypted = "e"
flag_block = "b"

mpq = "mpq"
zip_level = NUMBER
zip_bits = NUMBER / mpq
zip_level_and_bits = BEGIN zip_level COMMA zip_bits END
zip_args = zip_level / zip_level_and_bits

encryption_key = HEX_NUMBER
encryption_iv = HEX_NUMBER
encryption_args = BEGIN encryption_key COMMA encryption_iv COMMA espec END

unit_kilobyte = "K"
unit_megabyte = "M"
block_unit = unit_kilobyte / unit_megabyte
block_count = NUMBER
block_size = NUMBER (block_unit)?
block_size_args = STAR (block_count)?
block_size_spec = (block_size block_size_args?) / STAR
block_subchunk = block_size_spec EQUALS espec
block_args = block_subchunk / (BEGIN block_subchunk (COMMA block_subchunk)* END)

NUMBER = ~"[0-9]+"
HEX_NUMBER = ~"[0-9A-F]+"
COLON = ":"
COMMA = ","
EQUALS = "="
STAR = "*"
BEGIN = "{"
END = "}"
""")


class Frame:
	@classmethod
	def from_node(cls, node):
		raise NotImplementedError


class BlockTableFrame(Frame):
	@classmethod
	def from_node(cls, node):
		return cls()


class EncryptedFrame(Frame):
	@classmethod
	def from_node(cls, node):
		return cls()


class RawFrame(Frame):
	@classmethod
	def from_node(cls, node):
		return cls()


class ZipFrame(Frame):
	@classmethod
	def from_node(cls, node):
		return cls()


class EncodingSpec:
	def __init__(self, spec: str) -> None:
		self.spec = spec
		self.nodes = GRAMMAR.parse(spec)
		top_level_node = self.nodes.children[0]

		cls: Type[Frame]
		if top_level_node.expr_name == "data_raw":
			cls = RawFrame
		elif top_level_node.expr_name == "data_zipped":
			cls = ZipFrame
		elif top_level_node.expr_name == "data_encrypted":
			cls = EncryptedFrame
		elif top_level_node.expr_name == "data_block":
			cls = BlockTableFrame

		self.top_level_block = cls.from_node(top_level_node)
