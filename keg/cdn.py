import json
import os
from typing import IO
from urllib.parse import urljoin

import requests

from .archive import Archive, ArchiveIndex
from .configfile import BuildConfig, CDNConfig, PatchConfig
from .exceptions import NetworkError
from .utils import partition_hash, verify_data


DEFAULT_CONFIG_PATH = "tpr/configs/data"


def get_config_path(key: str) -> str:
	return f"/config/{partition_hash(key)}"


def get_data_path(key: str) -> str:
	return f"/data/{partition_hash(key)}"


def get_data_index_path(key: str) -> str:
	return get_data_path(key) + ".index"


def get_patch_path(key: str) -> str:
	return f"/patch/{partition_hash(key)}"


def get_patch_index_path(key: str) -> str:
	return get_patch_path(key) + ".index"


def get_config_item_path(key: str) -> str:
	return f"/{partition_hash(key)}"


class BaseCDN:
	def get_item(self, path: str) -> IO:
		raise NotImplementedError()

	def get_config_item(self, path: str) -> IO:
		raise NotImplementedError()

	def fetch_config(self, key: str, verify: bool=False) -> bytes:
		with self.get_item(get_config_path(key)) as resp:
			data = resp.read()
		verify_data("config file", data, key, verify)
		return data

	def fetch_config_data(self, key: str, verify: bool=False) -> bytes:
		with self.get_config_item(get_config_item_path(key)) as resp:
			data = resp.read()
		verify_data("config item", data, key, verify)
		return data

	def fetch_index(self, key: str, verify: bool=False) -> bytes:
		with self.get_item(get_data_index_path(key)) as resp:
			return resp.read()

	def fetch_patch(self, key: str, verify: bool=False) -> bytes:
		with self.get_item(get_patch_path(key)) as resp:
			data = resp.read()
		verify_data("patch file", data, key, verify)
		return data

	def fetch_patch_index(self, key: str, verify: bool=False) -> bytes:
		with self.get_item(get_patch_index_path(key)) as resp:
			data = resp.read()
		verify_data("patch index", data[-28:], key, verify)
		return data

	def get_build_config(self, key: str, verify: bool=False) -> BuildConfig:
		return BuildConfig.from_bytes(self.fetch_config(key, verify=verify))

	def get_cdn_config(self, key: str, verify: bool=False) -> CDNConfig:
		return CDNConfig.from_bytes(self.fetch_config(key, verify=verify))

	def get_patch_config(self, key: str, verify: bool=False) -> PatchConfig:
		return PatchConfig.from_bytes(self.fetch_config(key, verify=verify))

	def get_product_config(self, key: str, verify: bool=False) -> dict:
		return json.loads(self.fetch_config_data(key, verify))

	def get_archive(self, key: str) -> Archive:
		return Archive(key, self)

	def get_index(self, key: str, verify: bool=False) -> ArchiveIndex:
		return ArchiveIndex(self.fetch_index(key), key, verify=verify)

	def download_data(self, key: str, verify: bool=False) -> IO:
		return self.get_item(get_data_path(key))


class RemoteCDN(BaseCDN):
	def __init__(self, server: str, path: str, config_path: str) -> None:
		self.server = server
		self.path = path
		self.config_path = config_path

	def _join_path(self, base_path: str, path: str):
		# Final path always has to end with a "/"
		# Actual path can't begin with a "/"
		# urljoin("/foo/bar", "baz") => "/foo/baz"
		# urljoin("/foo/bar/", "baz") => "/foo/bar/baz"
		# urljoin("/foo/bar//", "baz") => "/foo/bar/baz"
		# urljoin("/foo/bar/", "/baz") => "/baz"
		return urljoin(base_path + "/", path.lstrip("/"))

	def get_response(self, path: str) -> requests.Response:
		url = urljoin(self.server, path)
		ret = requests.get(url, stream=True)
		if ret.status_code != 200:
			raise NetworkError(f"Unexpected status code {ret.status_code} for {url}")
		return ret

	def get_item(self, path: str) -> IO:
		final_path = self._join_path(self.path, path)
		return self.get_response(final_path).raw

	def get_config_item(self, path: str) -> IO:
		final_path = self._join_path(self.config_path, path)
		return self.get_response(final_path).raw


class LocalCDN(BaseCDN):
	def __init__(self, base_dir: str, fragments_dir: str) -> None:
		self.base_dir = base_dir
		self.fragments_dir = fragments_dir

	def get_full_path(self, path: str) -> str:
		return os.path.join(self.base_dir, path.lstrip("/"))

	def get_config_path(self, path: str) -> str:
		return os.path.join(
			self.base_dir, "configs", "data", path.lstrip("/")
		)

	def get_fragment_path(self, key: str) -> str:
		return os.path.join(self.fragments_dir, partition_hash(key))

	def get_item(self, path: str) -> IO:
		return open(self.get_full_path(path), "rb")

	def get_config_item(self, path: str) -> IO:
		return open(self.get_config_path(path), "rb")

	def get_fragment(self, key: str) -> IO:
		return open(self.get_fragment_path(key), "rb")

	def exists(self, path: str) -> bool:
		return os.path.exists(self.get_full_path(path))

	def has_config(self, key: str) -> bool:
		return self.exists(get_config_path(key))

	def has_data(self, key: str) -> bool:
		return self.exists(get_data_path(key))

	def has_index(self, key: str) -> bool:
		return self.exists(get_data_index_path(key))

	def has_patch(self, key: str) -> bool:
		return self.exists(get_patch_path(key))

	def has_patch_index(self, key: str) -> bool:
		return self.exists(get_patch_index_path(key))

	def has_config_item(self, key: str) -> bool:
		return os.path.exists(self.get_config_path(f"/{partition_hash(key)}"))

	def has_fragment(self, key: str) -> bool:
		return os.path.exists(self.get_fragment_path(key))

	def save_item(self, item: IO, path: str) -> None:
		cache_file_path = self.get_full_path(path)
		f = HTTPCacheWrapper(item, cache_file_path)
		f.close()


class CacheableCDNWrapper(BaseCDN):
	def __init__(
		self,
		base_dir: str,
		server: str,
		path: str,
		fragments_path: str,
		config_path: str=DEFAULT_CONFIG_PATH
	) -> None:
		if not os.path.exists(base_dir):
			os.makedirs(base_dir)
		self.local_cdn = LocalCDN(base_dir, fragments_path)
		self.remote_cdn = RemoteCDN(server, path, config_path)

	def get_item(self, path: str) -> IO:
		if not self.local_cdn.exists(path):
			remote_path = self.remote_cdn._join_path(self.remote_cdn.path, path)
			response = self.remote_cdn.get_response(remote_path)
			self.local_cdn.save_item(response.raw, path)

		return self.local_cdn.get_item(path)

	def get_config_item(self, path: str) -> IO:
		if not self.local_cdn.has_config_item(path):
			cache_file_path = self.local_cdn.get_config_path(path)
			remote_path = self.remote_cdn._join_path(self.remote_cdn.config_path, path)
			response = self.remote_cdn.get_response(remote_path)
			f = HTTPCacheWrapper(response.raw, cache_file_path)
			f.close()

		return self.local_cdn.get_config_item(path)


class HTTPCacheWrapper:
	def __init__(self, fp: IO, path: str) -> None:
		self.fp = fp

		dir_path = os.path.dirname(path)
		if not os.path.exists(dir_path):
			os.makedirs(dir_path)

		self._real_path = path
		self._temp_path = path + ".keg_temp"
		self._cache_file = open(self._temp_path, "wb")

	def __enter__(self):
		return self

	def __exit__(self, *exc):
		self.close()
		return False

	def close(self):
		self.read()
		self._cache_file.close()

		# Atomic write&move; make sure there's no partially-written caches.
		os.rename(self._temp_path, self._real_path)

		return self.fp.close()

	def read(self, size: int=-1) -> bytes:
		if size == -1:
			ret = self.fp.read()
		else:
			ret = self.fp.read(size)
		if ret:
			self._cache_file.write(ret)
		return ret
