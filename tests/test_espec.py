from keg import espec

from . import get_resource


def test_espec_grammar():
	with get_resource("encodings.txt", "r") as f:
		data = f.read()

	specs = data.splitlines()

	for spec in specs:
		espec.GRAMMAR.parse(spec)


def test_espec_zip_defaults():
	spec = espec.EncodingSpec("z")
	frame = spec.top_level_block
	assert isinstance(frame, espec.ZipFrame)
	assert frame.level == espec.ZipFrame.DEFAULT_LEVEL
	assert frame.bits == espec.ZipFrame.DEFAULT_BITS


def test_espec_zip_default_bits():
	spec = espec.EncodingSpec("z:6")
	frame = spec.top_level_block
	assert isinstance(frame, espec.ZipFrame)
	assert frame.level == 6
	assert frame.bits == espec.ZipFrame.DEFAULT_BITS


def test_espec_zip_mpq():
	spec = espec.EncodingSpec("z:{6,mpq}")
	frame = spec.top_level_block
	assert isinstance(frame, espec.ZipFrame)
	assert frame.level == 6
	assert frame.bits == 0


def test_espec_encrypted_raw():
	spec = espec.EncodingSpec("e:{A6D4CFE470214878,FD4466FC,n}")
	frame = spec.top_level_block
	assert isinstance(frame, espec.EncryptedFrame)
	assert frame.key == "A6D4CFE470214878"
	assert frame.nonce == "FD4466FC"
	assert isinstance(frame.subframe, espec.RawFrame)


def test_espec_encrypted_zip():
	spec = espec.EncodingSpec("e:{237DA26C65073F42,33F13F18,z}")
	frame = spec.top_level_block
	assert isinstance(frame, espec.EncryptedFrame)
	assert frame.key == "237DA26C65073F42"
	assert frame.nonce == "33F13F18"
	assert isinstance(frame.subframe, espec.ZipFrame)


def test_espec_equality():
	assert espec.RawFrame() == espec.RawFrame()
	assert espec.ZipFrame(9, 15) == espec.ZipFrame(9, 15)
	assert (
		espec.EncryptedFrame("237DA26C65073F42", "33F13F18", espec.RawFrame()) ==
		espec.EncryptedFrame("237DA26C65073F42", "33F13F18", espec.RawFrame())
	)
