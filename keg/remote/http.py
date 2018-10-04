import json
import os
from datetime import datetime
from hashlib import md5
from typing import Any, Tuple

import requests

from .. import psv
from ..exceptions import NetworkError
from ..utils import partition_hash
from .base import BaseRemote


class StatefulResponse:
	def __init__(self, path: str, response: requests.Response) -> None:
		self.path = path
		self.content = response.content
		self.timestamp = int(datetime.utcnow().timestamp())
		self.digest = md5(self.content).hexdigest()
		self.cache_path = os.path.join(
			self.path.strip("/"),
			partition_hash(self.digest)
		)

		if response.status_code != 200:
			raise NetworkError(f"Got status code {response.status_code} for {path!r}")


class HttpRemote(BaseRemote):
	supports_blobs = True

	def get_response(self, path: str) -> StatefulResponse:
		url = self.remote + path
		return StatefulResponse(path, requests.get(url))

	def get_blob(self, name: str) -> Tuple[Any, StatefulResponse]:
		resp = self.get_response(f"/blob/{name}")
		return json.loads(resp.content.decode()), resp

	def get_psv(self, name: str) -> Tuple[psv.PSVFile, StatefulResponse]:
		resp = self.get_response(f"/{name}")
		return psv.loads(resp.content.decode()), resp
