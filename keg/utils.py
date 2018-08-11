def partition_hash(hash: str) -> str:
	if len(hash) < 4:
		raise ValueError(f"Invalid hash to partition: {repr(hash)}")
	return f"{hash[0:2]}/{hash[2:4]}/{hash}"
