import socket
from datetime import datetime
from email.parser import BytesParser, HeaderParser
from hashlib import sha256
from urllib.parse import urlparse

from .exceptions import IntegrityVerificationError, NoDataError, RibbitError


DEFAULT_PORT = 1119


class RibbitRequest:
	"""
	A request to a Ribbit server.

	Initialize the request with hostname, port and encoded data to send.
	Perform the request with request.send()
	"""

	def __init__(self, hostname: str, port: int, path: str) -> None:
		self.hostname = hostname
		self.port = port
		self.path = path
		self.data = f"{path}\n".encode()

	def send(self, buffer_size: int) -> "RibbitResponse":
		# Connect to the ribbit server
		s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
		s.connect((self.hostname, self.port))
		buf = []
		try:
			# Send the path request
			s.send(self.data)
			# Receive and buffer the data
			chunk = s.recv(buffer_size)
			while chunk:
				buf.append(chunk)
				chunk = s.recv(buffer_size)
		finally:
			s.close()
		data = b"".join(buf)

		if not data:
			raise NoDataError(f"No data at {self.path}")

		# Data is expected to terminate in a CRLF, otherwise it's most likely broken
		if not data.endswith(b"\r\n"):
			raise RibbitError("Unterminated data... try again.")

		return RibbitResponse(self, data)


class RibbitResponse:
	"""
	A response to a RibbitRequest.

	The request that created that response is available on the .request attribute.
	"""

	def __init__(
		self, request: RibbitRequest, data: bytes, *, verify: bool = True
	) -> None:
		self.request = request
		self.data = data
		self.date = datetime.utcnow()

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
	"""
	A Ribbit client. Corresponds to a hostname/port pair.
	"""

	def __init__(self, hostname: str, port: int) -> None:
		self.hostname = hostname
		self.port = port

	def get(self, path: str, *, buffer_size: int = 4096) -> RibbitResponse:
		request = RibbitRequest(self.hostname, self.port, path)
		return request.send(buffer_size)


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
