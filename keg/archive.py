from io import BytesIO


class Archive:
	pass


class ArchiveIndex:
	def __init__(self, data: bytes, hash: str, verify: bool=False) -> None:
		self.data = BytesIO(data.read())
		self.hash = hash
		self.verify = verify


class ArchiveGroup:
	pass
