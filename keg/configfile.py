from typing import Dict, Iterable, Tuple, Type, TypeVar

from . import blizini
from .patch import PatchEntry


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
		self.install = self._values.get("install", "")
		self.download = self._values.get("download", "")
		self.patch = self._values.get("patch", "")
		self.patch_config = self._values.get("patch-config", "")
		self.build_name = self._values.get("build-name", "")
		self.build_product = self._values.get("build-product", "")
		self.build_uid = self._values.get("build-uid", "")

	@property
	def encodings(self) -> Tuple[str, str]:
		ret = self._values.get("encoding", "").split()[:2]
		if not ret:
			return "", ""
		elif len(ret) == 1:
			return ret[0], ""
		return ret[0], ret[1]


class CDNConfig(BaseConfig):
	def __init__(self, _values) -> None:
		super().__init__(_values)
		self.archive_group = self._values.get("archive-group", "")
		self.patch_archive_group = self._values.get("patch-archive-group", "")

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
