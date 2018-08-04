class BlizIni:
	def __init__(self):
		self.items = {}

	def read_string(self, text: str):
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


def load(text: str):
	p = BlizIni()
	p.read_string(text)

	return p.items
