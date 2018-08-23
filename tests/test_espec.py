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
