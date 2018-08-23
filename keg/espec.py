from typing import List, Tuple, Type

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
block_subchunk_short = block_size_spec EQUALS espec
block_subchunk_long = (BEGIN block_subchunk_short (COMMA block_subchunk_short)* END)
block_args = block_subchunk_short / block_subchunk_long

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


FrameInfo = Tuple[int, int, Frame]


def _get_shortform_block_frame_info(node) -> FrameInfo:
	size_spec_node = node.children[0]
	if size_spec_node.text == "*":
		# eg: b:{22=n,54=z,160=n,20480=n,128=n,16384=n,*=z}
		block_size = -1
		repeat = -1
	else:
		block_size_node = size_spec_node.children[0].children[0]

		block_size = int(block_size_node.children[0].text)
		block_size_unit = block_size_node.children[1].text
		# Can be "K", "M" or ""
		if block_size_unit == "K":
			block_size *= 1024
		elif block_size_unit == "M":
			block_size *= 1024 * 1024
		else:
			assert not block_size_unit

		block_size_args_node = size_spec_node.children[0].children[1]
		if block_size_args_node.text == "":
			# Repeat not specified, defaults to 1
			repeat = 1
		else:
			repeat_num = block_size_args_node.children[0].children[1].text
			if repeat_num == "":
				repeat = -1
			else:
				repeat = int(repeat_num)

	subframe = get_frame_for_node(node.children[2].children[0])
	return block_size, repeat, subframe


class BlockTableFrame(Frame):
	@classmethod
	def from_node(cls, node):
		args_node = node.children[2]
		frame_info: List[FrameInfo] = []

		# Shortform:
		# <Node called "block_args" matching "64K*=n">
		# 	<Node called "block_subchunk_short" matching "64K*=n">
		# 		<Node called "block_size_spec" matching "64K*">
		# 			<Node matching "64K*">
		# 				<Node called "block_size" matching "64K">
		# 					<RegexNode called "NUMBER" matching "64">
		# 					<Node matching "K">
		# 						<Node called "block_unit" matching "K">
		# 							<Node called "unit_kilobyte" matching "K">
		# 				<Node matching "*">
		# 					<Node called "block_size_args" matching "*">
		# 						<Node called "STAR" matching "*">
		# 						<Node matching "">
		# 		<Node called "EQUALS" matching "=">
		# 		<Node called "espec" matching "n">
		# 			<Node called "flag_raw" matching "n">

		# Longform:
		# <Node called "block_args" matching "{16K*=z:{6,mpq}}">
		# 	<Node called "block_subchunk_long" matching "{16K*=z:{6,mpq}}">
		# 		<Node called "BEGIN" matching "{">
		# 		<Node called "block_subchunk_short" matching "16K*=z:{6,mpq}">
		# 			<Node called "block_size_spec" matching "16K*">
		# 				<Node matching "16K*">
		# 					<Node called "block_size" matching "16K">
		# 						<RegexNode called "NUMBER" matching "16">
		# 						<Node matching "K">
		# 							<Node called "block_unit" matching "K">
		# 								<Node called "unit_kilobyte" matching "K">
		# 					<Node matching "*">
		# 						<Node called "block_size_args" matching "*">
		# 							<Node called "STAR" matching "*">
		# 							<Node matching "">
		# 			<Node called "EQUALS" matching "=">
		# 			<Node called "espec" matching "z:{6,mpq}">
		# 				<Node called "data_zipped" matching "z:{6,mpq}">
		# 					<Node called "flag_zip" matching "z">
		# 					<Node matching ":{6,mpq}">
		# 						<Node matching ":{6,mpq}">
		# 							<Node called "COLON" matching ":">
		# 							<Node called "zip_args" matching "{6,mpq}">
		# 								<Node called "zip_level_and_bits" matching "{6,mpq}">
		# 									<Node called "BEGIN" matching "{">
		# 									<RegexNode called "NUMBER" matching "6">
		# 									<Node called "COMMA" matching ",">
		# 									<Node called "zip_bits" matching "mpq">
		# 										<Node called "mpq" matching "mpq">
		# 									<Node called "END" matching "}">
		# 		<Node matching "">
		# 		<Node called "END" matching "}">

		subchunk_node = args_node.children[0]
		if subchunk_node.expr_name == "block_subchunk_short":
			frame_info.append(_get_shortform_block_frame_info(subchunk_node))

		else:
			frame_info.append(_get_shortform_block_frame_info(subchunk_node.children[1]))
			for node in subchunk_node.children[2].children:
				frame_info.append(_get_shortform_block_frame_info(node.children[1]))

		return cls(frame_info)

	def __init__(self, frame_info):
		self.frame_info = frame_info

	def __eq__(self, other):
		if not isinstance(other, self.__class__):
			return False

		return self.frame_info == other.frame_info


class EncryptedFrame(Frame):
	@classmethod
	def from_node(cls, node):
		args_node = node.children[2]

		# <Node called "encryption_args" matching "{A6D4CFE470214878,FD4466FC,n}">
		# 	<Node called "BEGIN" matching "{">
		# 	<RegexNode called "HEX_NUMBER" matching "A6D4CFE470214878">
		# 	<Node called "COMMA" matching ",">
		# 	<RegexNode called "HEX_NUMBER" matching "FD4466FC">
		# 	<Node called "COMMA" matching ",">
		# 	<Node called "espec" matching "n">
		# 		<Node called "flag_raw" matching "n">
		# 	<Node called "END" matching "}">

		key = args_node.children[1].text
		nonce = args_node.children[3].text
		subframe = get_frame_for_node(args_node.children[5].children[0])

		return cls(key, nonce, subframe)

	def __init__(self, key: str, nonce: str, subframe: Frame) -> None:
		self.key = key
		self.nonce = nonce
		self.subframe = subframe

	def __repr__(self):
		return f"<{self.__class__.__name__}: {self.key} {self.nonce}>"

	def __eq__(self, other):
		if not isinstance(other, self.__class__):
			return False

		return (
			other.key == self.key and
			other.nonce == self.nonce and
			other.subframe == self.subframe
		)


class RawFrame(Frame):
	@classmethod
	def from_node(cls, node):
		return cls()

	def __eq__(self, other):
		return isinstance(other, self.__class__)


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

	def __init__(self, level: int=DEFAULT_LEVEL, bits: int=DEFAULT_BITS) -> None:
		self.level = level
		self.bits = bits

	def __eq__(self, other):
		if not isinstance(other, self.__class__):
			return False
		return other.level == self.level and other.bits == self.bits


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
		self.frame = get_frame_for_node(self.nodes.children[0])

	def __eq__(self, other):
		if not isinstance(other, EncodingSpec):
			return False
		return self.frame == other.frame
