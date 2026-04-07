//go:build !windows

package downloader

import "os"

func replaceFileAtomically(source, target string) error {
	return os.Rename(source, target)
}
