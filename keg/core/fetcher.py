from io import BytesIO
from typing import Generator, Optional, Set, Type

from .. import blte, cdn
from ..archive import ArchiveGroup
from ..armadillo import ArmadilloKey
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

	def __init__(self, key: str, fetcher: "Fetcher") -> None:
		self.key = key
		self.fetcher = fetcher

	def fetch(self, verify: bool=False) -> None:
		path = self.get_full_path(self.key)
		if not self.exists():
			item = self.fetcher.remote_cdn.get_item(path)
			if self.fetcher.decryption_key:
				item = BytesIO(self.fetcher.decryption_key.decrypt_object(self.key, item.read()))
			self.fetcher.local_cdn.save_item(item, path)

	def exists(self) -> bool:
		return self.key_exists(self.key, self.fetcher.local_cdn)


class ProductConfigFetchDirective(FetchDirective):
	@classmethod
	def key_exists(cls, key: str, local_cdn: cdn.LocalCDN) -> bool:
		return local_cdn.has_config_item(key)

	def fetch(self, verify: bool=False) -> None:
		path = cdn.get_config_item_path(self.key)
		if not self.exists():
			item = self.fetcher.remote_cdn.get_config_item(path)
			self.fetcher.local_cdn.save_item(item, path)


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


class FetchQueue:
	def __init__(self, directive_class: Type[FetchDirective]) -> None:
		self.directive_class = directive_class
		self._queue: Set[str] = set()
		self.drained = 0

	def add(self, key: str) -> None:
		if key:
			self._queue.add(key)

	def exists(self, key: str, local_cdn: cdn.LocalCDN) -> bool:
		return self.directive_class.key_exists(key, local_cdn)

	def drain(self, fetcher: "Fetcher") -> Generator[FetchDirective, None, None]:
		for key in sorted(self._queue):
			if not self.exists(key, fetcher.local_cdn):
				yield self.directive_class(key, fetcher)
				self.drained += 1
			self._queue.remove(key)


class Drain:
	def __init__(self, name: str, queue: FetchQueue, fetcher: "Fetcher") -> None:
		self.name = name
		self.queue = queue
		self.fetcher = fetcher

	def __repr__(self):
		return f"<{self.__class__.__name__}: {self.name} ({len(self)} items)>"

	def __len__(self):
		return len(self.queue._queue)

	def drain(self) -> Generator[FetchDirective, None, None]:
		yield from self.queue.drain(self.fetcher)


class Fetcher:
	def __init__(
		self,
		version: Versions,
		local_cdn: cdn.LocalCDN,
		remote_cdn: cdn.RemoteCDN,
		verify: bool=False
	) -> None:
		self.version = version
		self.local_cdn = local_cdn
		self.remote_cdn = remote_cdn
		self.verify = verify

		self.product_config_queue = FetchQueue(ProductConfigFetchDirective)
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
		self.product_config: Optional[dict] = None

		self.decryption_key: Optional[ArmadilloKey] = None

	def fetch_config(self) -> Generator[Drain, None, None]:
		product_config_key = self.version.product_config
		self.product_config_queue.add(product_config_key)
		yield Drain("product config", self.product_config_queue, self)
		if self.version.product_config and self.local_cdn.has_config_item(product_config_key):
			self.product_config = self.local_cdn.get_product_config(product_config_key)

		if self.product_config:
			decryption_key_name = self.product_config.get("all", {}).get("config", {}).get(
				"decryption_key_name", ""
			)
			if decryption_key_name:
				self.decryption_key = self.local_cdn.get_decryption_key(decryption_key_name)

		self.config_queue.add(self.version.build_config)
		self.config_queue.add(self.version.cdn_config)
		yield Drain("config items", self.config_queue, self)

		if self.local_cdn.has_config(self.version.build_config):
			self.build_config = self.local_cdn.get_build_config(
				self.version.build_config, verify=self.verify
			)

		if self.local_cdn.has_config(self.version.cdn_config):
			self.cdn_config = self.local_cdn.get_cdn_config(
				self.version.cdn_config, verify=self.verify
			)

		if self.build_config:
			patch_config_key = self.build_config.patch_config
			if patch_config_key:
				self.config_queue.add(patch_config_key)
				yield Drain("patch config", self.config_queue, self)
				if self.local_cdn.has_config(patch_config_key):
					self.patch_config = self.local_cdn.get_patch_config(
						patch_config_key, verify=self.verify
					)

	def fetch_metadata(self) -> Generator[Drain, None, None]:
		yield from self.fetch_config()

		if self.cdn_config:
			for archive_key in self.cdn_config.archives:
				self.archive_queue.add(archive_key)
				self.index_queue.add(archive_key)

			if self.cdn_config.file_index:
				self.index_queue.add(self.cdn_config.file_index)

			yield Drain("archive indices", self.index_queue, self)

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

			encoding = self.build_config.encoding
			if encoding.encoding_key:
				self.loose_file_queue.add(encoding.encoding_key)
				yield Drain("encoding file", self.loose_file_queue, self)
				if self.local_cdn.has_data(encoding.encoding_key):
					data = blte.load(
						self.local_cdn.download_data(encoding.encoding_key),
						encoding.encoding_key,
						verify=self.verify
					)
					self.encoding_file = EncodingFile(
						data, self.build_config.encoding.content_key, verify=self.verify
					)

			if self.build_config.size.encoding_key:
				self.loose_file_queue.add(self.build_config.size.encoding_key)
				yield Drain("size file", self.loose_file_queue, self)

		if self.patch_index_queue:
			yield Drain("patch indices", self.patch_index_queue, self)

	def fetch_data(self) -> Generator[Drain, None, None]:
		if self.cdn_config:
			archive_group = ArchiveGroup(
				self.cdn_config.archives,
				self.cdn_config.archive_group,
				self.local_cdn,
				verify=self.verify
			)
			if self.encoding_file:
				for key, spec in self.encoding_file.encoding_keys:
					if not archive_group.has_file(key):
						self.loose_file_queue.add(key)

		yield Drain("archives", self.archive_queue, self)
		yield Drain("loose files", self.loose_file_queue, self)
		yield Drain("patch files", self.patch_queue, self)
