from collections import namedtuple
from typing import Dict, Iterable, Type, TypeVar

from . import blizini
from .patch import PatchEntry


# A content/encoding key pair
KeyPair = namedtuple("KeyPair", ["content_key", "encoding_key"])


def parse_key_pair(value: str) -> KeyPair:
	"""
	Parse a string that contains two or less hashes into a KeyPair
	"""
	pair = value.split()
	if len(pair) > 2:
		raise ValueError(f"Invalid KeyPair: {repr(pair)}")
	elif len(pair) == 2:
		content_key, encoding_key = pair
	elif len(pair) == 1:
		content_key, encoding_key = pair[0], ""
	elif not pair:
		content_key, encoding_key = "", ""

	return KeyPair(content_key=content_key, encoding_key=encoding_key)


ConfigFile = TypeVar("ConfigFile", bound="BaseConfig")


class BaseConfig:
	@classmethod
	def from_bytes(cls: Type[ConfigFile], data: bytes, verify: bool=False) -> ConfigFile:
		return cls(blizini.load(data.decode()))

	def __init__(self, _values: Dict[str, str]) -> None:
		self._values = _values

	def __repr__(self):
		return f"<{self.__class__.__name__}: {self._values}>"


class BuildConfig(BaseConfig):
	def __init__(self, _values):
		super().__init__(_values)
		self.root = self._values.get("root", "")
		self.install = parse_key_pair(self._values.get("install", ""))
		self.download = parse_key_pair(self._values.get("download", ""))
		self.size = parse_key_pair(self._values.get("size", ""))  # Size file
		self.encoding = parse_key_pair(self._values.get("encoding", ""))
		self.patch = self._values.get("patch", "")
		self.patch_config = self._values.get("patch-config", "")
		self.build_name = self._values.get("build-name", "")
		self.build_product = self._values.get("build-product", "")
		self.build_uid = self._values.get("build-uid", "")


class CDNConfig(BaseConfig):
	def __init__(self, _values) -> None:
		super().__init__(_values)
		self.archive_group = self._values.get("archive-group", "")
		self.patch_archive_group = self._values.get("patch-archive-group", "")
		self.file_index = self._values.get("file-index", "")
		self.patch_file_index = self._values.get("patch-file-index", "")

	@property
	def archives(self):
		return self._values.get("archives", "").split()

	@property
	def patch_archives(self):
		return self._values.get("patch-archives", "").split()


class PatchConfig(BaseConfig):
	def __init__(self, _values) -> None:
		super().__init__(_values)
		self.patch = self._values.get("patch", "")

	@property
	def patch_entries(self) -> Iterable[PatchEntry]:
		for entry in self._values.get("patch-entry", "").splitlines():
			yield PatchEntry(entry)

	@property
	def patch_size(self):
		return int(self._values.get("patch-size", "0"))
