import json
import os
from datetime import datetime
from hashlib import md5
from io import StringIO
from typing import Any, List, Tuple

import requests

from . import psv, psvresponse
from .exceptions import NetworkError
from .utils import partition_hash


class StatefulResponse:
	def __init__(self, name: str, response: requests.Response) -> None:
		self.name = name
		self.content = response.content
		self.timestamp = int(datetime.now().timestamp())
		self.digest = md5(self.content).hexdigest()
		self.cache_path = os.path.join(
			self.name.strip("/"),
			partition_hash(self.digest)
		)

		if response.status_code != 200:
			raise NetworkError(f"Got status code {response.status_code} for {repr(name)}")


class Remote:
	pass


class HttpRemote(Remote):
	def __init__(self, remote: str) -> None:
		self.remote = remote

	def get_response(self, path: str) -> StatefulResponse:
		url = self.remote + path
		return StatefulResponse(path, requests.get(url))

	def get_blobs(self) -> List[psvresponse.Blobs]:
		psvfile, _ = self.get_psv("/blobs")
		return [psvresponse.Blobs(row) for row in psvfile]

	def get_cdns(self) -> List[psvresponse.CDNs]:
		psvfile, _ = self.get_psv("/cdns")
		return [psvresponse.CDNs(row) for row in psvfile]

	def get_versions(self) -> List[psvresponse.Versions]:
		psvfile, _ = self.get_psv("/versions")
		return [psvresponse.Versions(row) for row in psvfile]

	def get_blob(self, name: str) -> Tuple[Any, StatefulResponse]:
		resp = self.get_response(f"/blob/{name}")
		return json.loads(resp.content.decode()), resp

	def get_bgdl(self) -> List[psvresponse.BGDL]:
		psvfile, _ = self.get_psv("/bgdl")
		return [psvresponse.BGDL(row) for row in psvfile]

	def get_psv(self, path: str) -> Tuple[psv.PSVFile, StatefulResponse]:
		resp = self.get_response(path)
		return psv.load(StringIO(resp.content.decode())), resp
