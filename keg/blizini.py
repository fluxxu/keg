from typing import Dict


class BlizIni:
	def __init__(self) -> None:
		self.items: Dict[str, str] = {}

	def read_string(self, text: str) -> None:
		for line in text.splitlines():
			line = line.strip()
			if not line or line.startswith("#"):
				continue
			key, _, value = line.partition("=")
			key = key.strip()
			value = value.strip()

			if key in self.items:
				self.items[key] += "\n" + value
			else:
				self.items[key] = value


def load(text: str) -> Dict[str, str]:
	p = BlizIni()
	p.read_string(text)

	return p.items
