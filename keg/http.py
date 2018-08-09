from io import StringIO
from typing import List

import requests

from . import psv


class CDNs:
	def __init__(self, values: psv.PSVRow) -> None:
		self._values = values
		self.name = values.get("Name", "")
		self.path = values.get("Path", "")
		self.config_path = values.get("ConfigPath", "")

	def __repr__(self):
		return f"<{self.__class__.__name__}: {self._values}>"

	@property
	def all_servers(self) -> List[str]:
		return self.servers + [f"http://{host}" for host in self.hosts]

	@property
	def hosts(self) -> List[str]:
		return self._values.get("Hosts", "").split()

	@property
	def servers(self) -> List[str]:
		return self._values.get("Servers", "").split()


class Versions:
	def __init__(self, values: psv.PSVRow) -> None:
		self._values = values
		self.build_config = values.get("BuildConfig", "")
		self.build_id = values.get("BuildId", "")
		self.cdn_config = values.get("CDNConfig", "")
		self.keyring = values.get("KeyRing", "")
		self.product_config = values.get("ProductConfig", "")
		self.region = values.get("Region", "")
		self.versions_name = values.get("VersionsName", "")

	def __repr__(self):
		return f"<{self.__class__.__name__}: {self._values}>"


class HttpBackend:
	def __init__(self, remote: str) -> None:
		self.remote = remote

	def request_path(self, path: str) -> requests.Response:
		url = self.remote + path
		return requests.get(url)

	def get_cdns(self) -> List[CDNs]:
		return [CDNs(row) for row in self.get_psv("/cdns")]

	def get_versions(self) -> List[Versions]:
		return [Versions(row) for row in self.get_psv("/versions")]

	def get_bytes(self, path: str) -> bytes:
		return self.request_path(path).content

	def get_psv(self, path: str) -> psv.PSVFile:
		return psv.load(
			StringIO(self.get_bytes(path).decode())
		)
