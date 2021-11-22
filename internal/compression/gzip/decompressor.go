package gzip

import (
	"compress/gzip"
	"github.com/pkg/errors"
	"github.com/wal-g/wal-g/utility"
	"io"
)

type Decompressor struct{}

const FileExtension = "gz"

func (decompressor Decompressor) Decompress(dst io.Writer, src io.Reader) error {
	gzReader, err := gzip.NewReader(src)
	if err != nil {
		return err
	}

	_, err = utility.FastCopy(dst, gzReader)
	return errors.Wrap(err, "DecompressGzip: gzip write failed")
}

func (decompressor Decompressor) FileExtension() string {
	return FileExtension
}
