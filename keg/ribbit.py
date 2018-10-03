import socket
from email.parser import BytesParser, HeaderParser
from hashlib import sha256
from urllib.parse import urlparse

from .exceptions import IntegrityVerificationError


DEFAULT_PORT = 1119


class RibbitError(Exception):
	pass


class RibbitResponse:
	def __init__(self, data: bytes, *, verify: bool = True) -> None:
		self.data = data

		self.message = BytesParser().parsebytes(data)  # type: ignore # (typeshed#2502)
		self.checksum = parse_checksum(self.message.epilogue)

		# The bytes of everything except the checksum (the epilogue)
		# The checksum is of those bytes
		self.content_bytes = data[:-len(self.message.epilogue)]
		if verify:
			content_checksum = sha256(self.content_bytes).hexdigest()
			if self.checksum != content_checksum:
				raise IntegrityVerificationError("ribbit response", content_checksum, self.checksum)

		self.content = self.message.get_payload(0).get_payload()
		self.signature = self.message.get_payload(1).get_payload()
		# TODO: verify signature as well


class RibbitClient:
	def __init__(self, hostname: str, port: int) -> None:
		self.hostname = hostname
		self.port = port

	def get(self, path: str, *, buffer_size: int = 4096) -> RibbitResponse:
		# Connect to the ribbit server
		s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
		s.connect((self.hostname, self.port))
		buf = []
		try:
			# Send the path request
			s.send(path.encode() + b"\n")
			# Receive and buffer the data
			chunk = s.recv(buffer_size)
			while chunk:
				buf.append(chunk)
				chunk = s.recv(buffer_size)
		finally:
			s.close()
		data = b"".join(buf)

		if not data:
			raise RibbitError(f"No data at {path!r}")

		# Data is expected to terminate in a CRLF, otherwise it's most likely broken
		if not data.endswith(b"\r\n"):
			raise RibbitError("Unterminated data... try again.")

		return RibbitResponse(data)


def parse_checksum(header: str) -> str:
	"""
	Parse the Checksum header (eg. from the email epilogue)
	"""
	# Epilogue example:
	# "Checksum: e231f8e724890aca477ca5efdfc7bc9c31e1da124510b4f420ebcf9c2d1fbe74\r\n"
	msg = HeaderParser().parsestr(header)
	return msg["Checksum"]  # type: ignore


def get(url: str, **kwargs) -> RibbitResponse:
	"""
	Query a ribbit url. Returns a RibbitResponse object.
	Port defaults to 1119 if not specified.

	Usage example:
	>>> from keg import ribbit
	>>> ribbit.get("ribbit://version.example.com/v1/products/foo/cdns")
	"""

	u = urlparse(url)
	if u.scheme != "ribbit":
		raise ValueError(f"Invalid ribbit url: {url!r} (must start with ribbit://)")
	client = RibbitClient(u.hostname, u.port or DEFAULT_PORT)

	return client.get(u.path.lstrip("/"), **kwargs)
