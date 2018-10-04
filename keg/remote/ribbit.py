from typing import Tuple
from urllib.parse import urlparse

from .. import psv, ribbit
from .base import BaseRemote


class RibbitRemote(BaseRemote):
	supports_blobs = False

	def __init__(self, remote: str, verify: bool = False) -> None:
		super().__init__(remote)

		url = urlparse(remote)
		if url.scheme != "ribbit":
			raise ValueError(f"Invalid ribbit url: {url!r} (must start with ribbit://)")

		# Store a cleaned remote, removing the path
		self.base_remote = url._replace(path="").geturl()

		self.product = url.path.lstrip("/")

	def get_response(self, path: str) -> ribbit.RibbitResponse:
		return ribbit.get(self.base_remote + "/" + path)

	def get_blob(self, name: str):
		raise NotImplementedError(f"Blob {name!r} is not available on Ribbit")

	def get_blobs(self):
		raise NotImplementedError("Blobs are not available on Ribbit")

	def get_psv(self, name: str) -> Tuple[psv.PSVFile, ribbit.RibbitResponse]:
		path = f"v1/products/{self.product}/{name}"
		response = self.get_response(path)
		psvfile = psv.loads(response.content)

		return psvfile, response
