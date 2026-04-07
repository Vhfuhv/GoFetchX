//go:build windows

package downloader

import (
	"os"
	"syscall"
	"unsafe"
)

const (
	moveFileReplaceExisting = 0x1
	moveFileWriteThrough    = 0x8
)

var (
	kernel32        = syscall.NewLazyDLL("kernel32.dll")
	procMoveFileExW = kernel32.NewProc("MoveFileExW")
)

func replaceFileAtomically(source, target string) error {
	srcPtr, err := syscall.UTF16PtrFromString(source)
	if err != nil {
		return err
	}
	dstPtr, err := syscall.UTF16PtrFromString(target)
	if err != nil {
		return err
	}

	r1, _, callErr := procMoveFileExW.Call(
		uintptr(unsafe.Pointer(srcPtr)),
		uintptr(unsafe.Pointer(dstPtr)),
		uintptr(moveFileReplaceExisting|moveFileWriteThrough),
	)
	if r1 == 0 {
		if callErr != nil && callErr != syscall.Errno(0) {
			return os.NewSyscallError("MoveFileExW", callErr)
		}
		return os.NewSyscallError("MoveFileExW", syscall.EINVAL)
	}

	return nil
}
