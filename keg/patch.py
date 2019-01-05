from typing import List, Tuple


class PatchEntry:
	def __init__(self, raw: str) -> None:
		tokens = raw.split()
		self.type = tokens.pop(0)
		self.content_hash = tokens.pop(0)
		self.content_size = int(tokens.pop(0))
		self.encoding_key = tokens.pop(0)
		self.encoded_size = int(tokens.pop(0))
		self.encoding_format = tokens.pop(0)
		self.pairs: List[Tuple[str, int, str, int]] = []

		while tokens:
			self.pairs.append(
				(tokens.pop(0), int(tokens.pop(0)), tokens.pop(0), int(tokens.pop(0)))
			)
