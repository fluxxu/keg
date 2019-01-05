from keg import espec

from . import get_resource


def test_espec_grammar():
	with get_resource("encodings.txt", "r") as f:
		data = f.read()

	specs = data.splitlines()

	for spec in specs:
		espec.EncodingSpec(spec)


def test_espec_zip_defaults():
	spec = espec.EncodingSpec("z")
	frame = spec.frame
	assert isinstance(frame, espec.ZipFrame)
	assert frame.level == espec.ZipFrame.DEFAULT_LEVEL
	assert frame.bits == espec.ZipFrame.DEFAULT_BITS


def test_espec_zip_default_bits():
	spec = espec.EncodingSpec("z:6")
	frame = spec.frame
	assert isinstance(frame, espec.ZipFrame)
	assert frame.level == 6
	assert frame.bits == espec.ZipFrame.DEFAULT_BITS


def test_espec_zip_mpq():
	spec = espec.EncodingSpec("z:{6,mpq}")
	frame = spec.frame
	assert isinstance(frame, espec.ZipFrame)
	assert frame.level == 6
	assert frame.bits == 0


def test_espec_encrypted_raw():
	spec = espec.EncodingSpec("e:{A6D4CFE470214878,FD4466FC,n}")
	frame = spec.frame
	assert isinstance(frame, espec.EncryptedFrame)
	assert frame.key == "A6D4CFE470214878"
	assert frame.nonce == "FD4466FC"
	assert isinstance(frame.subframe, espec.RawFrame)


def test_espec_encrypted_zip():
	spec = espec.EncodingSpec("e:{237DA26C65073F42,33F13F18,z}")
	frame = spec.frame
	assert isinstance(frame, espec.EncryptedFrame)
	assert frame.key == "237DA26C65073F42"
	assert frame.nonce == "33F13F18"
	assert isinstance(frame.subframe, espec.ZipFrame)


def test_espec_block_shortform():
	spec = espec.EncodingSpec("b:64K=n")
	frame = spec.frame

	assert isinstance(frame, espec.BlockTableFrame)
	assert frame.frame_info == [(65536, 1, espec.RawFrame())]


def test_espec_block_shortform_repeat():
	spec = espec.EncodingSpec("b:64K*2=n")
	frame = spec.frame

	assert isinstance(frame, espec.BlockTableFrame)
	assert frame.frame_info == [(65536, 2, espec.RawFrame())]


def test_espec_block_shortform_repeat_eof():
	spec = espec.EncodingSpec("b:64K*=n")
	frame = spec.frame

	assert isinstance(frame, espec.BlockTableFrame)
	assert frame.frame_info == [(65536, -1, espec.RawFrame())]


def test_espec_block_longform():
	spec = espec.EncodingSpec("b:{1898=z,51570=n}")
	frame = spec.frame

	assert isinstance(frame, espec.BlockTableFrame)
	assert frame.frame_info == [(1898, 1, espec.ZipFrame()), (51570, 1, espec.RawFrame())]


def test_espec_longform_oneframe():
	spec = espec.EncodingSpec("b:{16K*=z:{6,mpq}}")
	frame = spec.frame

	assert isinstance(frame, espec.BlockTableFrame)
	assert frame.frame_info == [(16 * 1024, -1, espec.ZipFrame(level=6, bits=0))]


def test_espec_block_longform_manyframes():
	spec = espec.EncodingSpec("b:{128=z:6,32768=z:6,8192=z:6,2768=z:6,64K*=z:6}")
	frame = spec.frame

	assert isinstance(frame, espec.BlockTableFrame)
	assert frame.frame_info == [
		(128, 1, espec.ZipFrame(level=6)),
		(32768, 1, espec.ZipFrame(level=6)),
		(8192, 1, espec.ZipFrame(level=6)),
		(2768, 1, espec.ZipFrame(level=6)),
		(65536, -1, espec.ZipFrame(level=6)),
	]


def test_espec_block_longform_manyframes_unspecified_zipped():
	spec = espec.EncodingSpec("b:{22=n,54=z,160=n,20480=n,128=n,16384=n,*=z}")
	frame = spec.frame

	assert isinstance(frame, espec.BlockTableFrame)
	assert frame.frame_info == [
		(22, 1, espec.RawFrame()),
		(54, 1, espec.ZipFrame()),
		(160, 1, espec.RawFrame()),
		(20480, 1, espec.RawFrame()),
		(128, 1, espec.RawFrame()),
		(16384, 1, espec.RawFrame()),
		(-1, -1, espec.ZipFrame()),
	]


def test_espec_equality():
	assert espec.RawFrame() == espec.RawFrame()
	assert espec.ZipFrame(9, 15) == espec.ZipFrame(9, 15)
	assert espec.EncryptedFrame(
		"237DA26C65073F42", "33F13F18", espec.RawFrame()
	) == espec.EncryptedFrame("237DA26C65073F42", "33F13F18", espec.RawFrame())

	assert espec.EncodingSpec("n") == espec.EncodingSpec("n")
	assert espec.EncodingSpec("n") != espec.EncodingSpec("z")

	assert espec.EncodingSpec("z") == espec.EncodingSpec("z")
	assert espec.EncodingSpec("z:9") == espec.EncodingSpec("z")
	assert espec.EncodingSpec("z:{9,15}") == espec.EncodingSpec("z")
	assert espec.EncodingSpec("z:9") != espec.EncodingSpec("z:10")
	assert espec.EncodingSpec("z:9") != espec.EncodingSpec("z:{9,mpq}")

	assert espec.EncodingSpec("e:{A6D4CFE470214878,FD4466FC,n}") == espec.EncodingSpec(
		"e:{A6D4CFE470214878,FD4466FC,n}"
	)

	# Shortform equality
	assert espec.EncodingSpec("b:64K=n") == espec.EncodingSpec("b:65536=n")
	assert espec.EncodingSpec("b:64K*1=n") == espec.EncodingSpec("b:65536=n")
	assert espec.EncodingSpec("b:64K*1=n") != espec.EncodingSpec("b:65536*=n")
	assert espec.EncodingSpec("b:64K*2=n") != espec.EncodingSpec("b:65536*1=n")
