from typing import IO, Any, Generator, Optional, Set, Type

from .. import blte, cdn
from ..archive import ArchiveGroup
from ..configfile import BuildConfig, CDNConfig, PatchConfig
from ..encoding import EncodingFile
from ..http import Versions


class FetchDirective:
	@classmethod
	def key_exists(cls, key: str, local_cdn: cdn.LocalCDN) -> bool:
		return local_cdn.exists(cls.get_full_path(key))

	@staticmethod
	def get_full_path(key: str) -> str:
		raise NotImplementedError()

	def __init__(
		self,
		key: str,
		local_cdn: cdn.LocalCDN,
		remote_cdn: cdn.RemoteCDN
	) -> None:
		self.key = key
		self.local_cdn = local_cdn
		self.remote_cdn = remote_cdn
		self.obj: Optional[Any] = None

	def get_object(self, item: IO, verify: bool=False) -> Optional[Any]:
		return None

	def fetch(self, verify: bool=False) -> IO:
		path = self.get_full_path(self.key)
		if not self.exists():
			item = self.remote_cdn.get_item(path)
			self.local_cdn.save_item(item, path)

		item = self.local_cdn.get_item(path)  # Reopen the item locally

		return item

	def exists(self) -> bool:
		return self.key_exists(self.key, self.local_cdn)


class ConfigFetchDirective(FetchDirective):
	get_full_path = staticmethod(cdn.get_config_path)  # type: ignore


class DataFetchDirective(FetchDirective):
	get_full_path = staticmethod(cdn.get_data_path)  # type: ignore


class DataIndexFetchDirective(FetchDirective):
	get_full_path = staticmethod(cdn.get_data_index_path)  # type: ignore


class PatchFetchDirective(FetchDirective):
	get_full_path = staticmethod(cdn.get_patch_path)  # type: ignore


class PatchIndexFetchDirective(FetchDirective):
	get_full_path = staticmethod(cdn.get_patch_index_path)  # type: ignore


class EncodingFetchDirective(DataFetchDirective):
	obj: EncodingFile

	def get_object(self, item: IO, verify: bool=False) -> EncodingFile:
		data = blte.load(item, self.key, verify=verify)
		return EncodingFile(data, self.key, verify=verify)


class BuildConfigFetchDirective(ConfigFetchDirective):
	obj: BuildConfig

	def get_object(self, item: IO, verify: bool=False) -> BuildConfig:
		return BuildConfig.from_bytes(item.read(), verify=verify)


class CDNConfigFetchDirective(ConfigFetchDirective):
	obj: CDNConfig

	def get_object(self, item: IO, verify: bool=False) -> CDNConfig:
		return CDNConfig.from_bytes(item.read(), verify=verify)


class PatchConfigFetchDirective(ConfigFetchDirective):
	obj: PatchConfig

	def get_object(self, item: IO, verify: bool=False) -> PatchConfig:
		return PatchConfig.from_bytes(item.read(), verify=verify)


class FetchQueue:
	def __init__(self, directive_class: Type[FetchDirective]) -> None:
		self.directive_class = directive_class
		self._queue: Set[str] = set()
		self.drained = 0

	def add(self, key: str) -> None:
		self._queue.add(key)

	def exists(self, key: str, local_cdn: cdn.LocalCDN) -> bool:
		return self.directive_class.key_exists(key, local_cdn)

	def drain(
		self,
		local_cdn: cdn.LocalCDN,
		remote_cdn: cdn.RemoteCDN
	) -> Generator[FetchDirective, None, None]:
		for key in sorted(self._queue):
			if not self.exists(key, local_cdn):
				yield self.directive_class(key, local_cdn, remote_cdn)
				self.drained += 1
			self._queue.remove(key)


class Drain:
	def __init__(
		self,
		name: str,
		queue: FetchQueue,
		local_cdn: cdn.LocalCDN,
		remote_cdn: cdn.RemoteCDN
	) -> None:
		self.name = name
		self.queue = queue
		self.local_cdn = local_cdn
		self.remote_cdn = remote_cdn

	def __repr__(self):
		return f"<{self.__class__.__name__}: {self.name} ({len(self)} items)>"

	def __len__(self):
		return len(self.queue._queue)

	def drain(self) -> Generator[FetchDirective, None, None]:
		yield from self.queue.drain(self.local_cdn, self.remote_cdn)


class Fetcher:
	def __init__(
		self,
		version: Versions,
		verify: bool=False
	) -> None:
		self.version = version
		self.verify = verify

		self.config_queue = FetchQueue(ConfigFetchDirective)
		self.index_queue = FetchQueue(DataIndexFetchDirective)
		self.patch_index_queue = FetchQueue(PatchIndexFetchDirective)
		self.archive_queue = FetchQueue(DataFetchDirective)
		self.loose_file_queue = FetchQueue(DataFetchDirective)
		self.patch_queue = FetchQueue(PatchFetchDirective)

		self.build_config: Optional[BuildConfig] = None
		self.cdn_config: Optional[CDNConfig] = None
		self.patch_config: Optional[PatchConfig] = None
		self.encoding_file: Optional[EncodingFile] = None

	def fetch_config(
		self,
		local_cdn: cdn.LocalCDN,
		remote_cdn: cdn.RemoteCDN
	) -> Generator[Drain, None, None]:
		self.config_queue.add(self.version.build_config)
		self.config_queue.add(self.version.cdn_config)
		yield Drain("config items", self.config_queue, local_cdn, remote_cdn)

		if local_cdn.has_config(self.version.build_config):
			self.build_config = local_cdn.get_build_config(
				self.version.build_config, verify=self.verify
			)

		if local_cdn.has_config(self.version.cdn_config):
			self.cdn_config = local_cdn.get_cdn_config(
				self.version.cdn_config, verify=self.verify
			)

		if self.build_config:
			patch_config_key = self.build_config.patch_config
			if patch_config_key:
				self.config_queue.add(patch_config_key)
				yield Drain("patch config", self.config_queue, local_cdn, remote_cdn)
				if local_cdn.has_config(patch_config_key):
					self.patch_config = local_cdn.get_patch_config(
						patch_config_key, verify=self.verify
					)

	def fetch_metadata(
		self,
		local_cdn: cdn.LocalCDN,
		remote_cdn: cdn.RemoteCDN
	) -> Generator[Drain, None, None]:
		yield from self.fetch_config(local_cdn, remote_cdn)

		if self.cdn_config:
			for archive_key in self.cdn_config.archives:
				self.archive_queue.add(archive_key)
				self.index_queue.add(archive_key)

			if self.cdn_config.file_index:
				self.index_queue.add(self.cdn_config.file_index)

			yield Drain("archive indices", self.index_queue, local_cdn, remote_cdn)

			for patch_archive_key in self.cdn_config.patch_archives:
				self.patch_queue.add(patch_archive_key)
				self.patch_index_queue.add(patch_archive_key)

			if self.cdn_config.patch_file_index:
				self.patch_index_queue.add(self.cdn_config.patch_file_index)

		if self.build_config:
			if self.patch_config:
				for patch_entry in self.patch_config.patch_entries:
					for old_key, old_size, patch_key, patch_size in patch_entry.pairs:
						self.patch_queue.add(patch_key)

			_, encoding_key = self.build_config.encodings
			if encoding_key:
				self.loose_file_queue.add(encoding_key)
				yield Drain("encoding file", self.loose_file_queue, local_cdn, remote_cdn)
				if local_cdn.has_data(encoding_key):
					data = blte.load(local_cdn.download_data(encoding_key), encoding_key)
					self.encoding_file = EncodingFile(data, encoding_key, verify=self.verify)

		if self.patch_index_queue:
			yield Drain("patch indices", self.patch_index_queue, local_cdn, remote_cdn)

	def fetch_data(
		self,
		local_cdn: cdn.LocalCDN,
		remote_cdn: cdn.RemoteCDN
	) -> Generator[Drain, None, None]:
		if self.cdn_config:
			archive_group = ArchiveGroup(
				self.cdn_config.archives,
				self.cdn_config.archive_group,
				local_cdn,
				verify=self.verify
			)
			if self.encoding_file:
				for key, spec in self.encoding_file.encoding_keys:
					if not archive_group.has_file(key):
						self.loose_file_queue.add(key)

		yield Drain("archives", self.archive_queue, local_cdn, remote_cdn)
		yield Drain("loose files", self.loose_file_queue, local_cdn, remote_cdn)
		yield Drain("patch files", self.patch_queue, local_cdn, remote_cdn)
