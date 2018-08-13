
class KegException(Exception):
	pass


class NetworkError(KegException):
	pass


class IntegrityVerificationError(KegException):
	def __init__(self, object_name: str, digest: str, expected_digest: str) -> None:
		message = (
			f"Integrity verification failed for {object_name}\n"
			f"Expected: {expected_digest}\n"
			f"Got:      {digest}"
		)
		super().__init__(message)
