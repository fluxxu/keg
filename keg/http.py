from io import StringIO
from typing import List, Tuple

import requests

from . import psv
from .exceptions import NetworkError


class PSVResponse:
	def __init__(self, row: psv.PSVRow) -> None:
		self._row = row

	def __repr__(self):
		return f"<{self.__class__.__name__}: {self._row}>"


class CDNs(PSVResponse):
	def __init__(self, row: psv.PSVRow) -> None:
		super().__init__(row)
		self.name = row.Name
		self.path = row.Path
		self.config_path = row.ConfigPath

	def __repr__(self):
		return f"<{self.__class__.__name__}: {self._values}>"

	@property
	def all_servers(self) -> List[str]:
		return self.servers + [f"http://{host}" for host in self.hosts]

	@property
	def hosts(self) -> List[str]:
		return self._row.Hosts.split()

	@property
	def servers(self) -> List[str]:
		return self._row.Servers.split()


class Versions(PSVResponse):
	def __init__(self, row: psv.PSVRow) -> None:
		super().__init__(row)
		self.build_config = row.BuildConfig
		self.build_id = row.BuildId
		self.cdn_config = row.CDNConfig
		self.keyring = row.KeyRing
		self.product_config = row.ProductConfig
		self.region = row.Region
		self.versions_name = row.VersionsName


class HttpBackend:
	def __init__(self, remote: str) -> None:
		self.remote = remote

	def request_path(self, path: str) -> requests.Response:
		url = self.remote + path
		return requests.get(url)

	def get_cdns(self) -> List[CDNs]:
		psvfile, _ = self.get_psv("/cdns")
		return [CDNs(row) for row in psvfile]

	def get_versions(self) -> List[Versions]:
		psvfile, _ = self.get_psv("/versions")
		return [Versions(row) for row in psvfile]

	def get_bytes(self, path: str) -> bytes:
		return self.request_path(path).content

	def get_psv(self, path: str) -> Tuple[psv.PSVFile, requests.Response]:
		resp = self.request_path(path)
		if resp.status_code != 200:
			raise NetworkError(f"Got status code {resp.status_code} for {repr(path)}")
		return psv.load(StringIO(resp.content.decode())), resp
