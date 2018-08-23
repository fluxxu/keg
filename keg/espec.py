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
	DEFAULT_LEVEL = 9
	DEFAULT_BITS = 15

	@classmethod
	def from_node(cls, node):
		if node.children[1].text == "":
			level = cls.DEFAULT_LEVEL
			bits = cls.DEFAULT_BITS
		else:
			args_node = node.children[1].children[0].children[1]
			if args_node.text.isdigit():
				level = int(args_node.text)
				bits = cls.DEFAULT_BITS
			else:
				level_and_bits_node = args_node.children[0]

				# <Node called "zip_level_and_bits" matching "{6,mpq}">
				# 	<Node called "BEGIN" matching "{">
				# 	<RegexNode called "NUMBER" matching "6">
				# 	<Node called "COMMA" matching ",">
				# 	<Node called "zip_bits" matching "mpq">
				# 		<Node called "mpq" matching "mpq">
				# 	<Node called "END" matching "}">

				level = int(level_and_bits_node.children[1].text)
				bits_node = level_and_bits_node.children[3]
				if bits_node.text == "mpq":
					bits = 0
				else:
					bits = int(bits_node.text)

		return cls(level, bits)

	def __init__(self, level: int, bits: int) -> None:
		self.level = level
		self.bits = bits


def get_frame_for_node(node) -> Frame:
	cls: Type[Frame]
	if node.expr_name in ("data_raw", "flag_raw"):
		cls = RawFrame
	elif node.expr_name == "data_zipped":
		cls = ZipFrame
	elif node.expr_name == "data_encrypted":
		cls = EncryptedFrame
	elif node.expr_name == "data_block":
		cls = BlockTableFrame
	else:
		raise ValueError(node.expr_name)

	return cls.from_node(node)  # type: ignore


class EncodingSpec:
	def __init__(self, spec: str) -> None:
		self.spec = spec
		self.nodes = GRAMMAR.parse(spec)
		top_level_node = self.nodes.children[0]
		self.top_level_block = get_frame_for_node(top_level_node)
