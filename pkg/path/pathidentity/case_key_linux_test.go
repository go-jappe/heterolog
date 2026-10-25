//go:build linux

package pathidentity

import (
	"bufio"
	"errors"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"golang.org/x/sys/unix"
)

// fakeLinuxFilesystemは、directoryごとのstatfsのf_type・casefold属性・st_devと、
// /proc/self/mountinfoの内容を差し替えます。登録のないdirectoryは未作成として扱い、
// statfsとstatはENOENTを返します。
type fakeLinuxFilesystem struct {
	directories    map[string]fakeLinuxDirectory
	mountInfo      string
	mountInfoErr   error
	statfsCalls    []string
	casefoldCalls  []string
	mountInfoReads int
}

type fakeLinuxDirectory struct {
	magic       uint32
	casefold    bool
	casefoldErr error
	device      linuxDevice
	deviceErr   error
}

func (f *fakeLinuxFilesystem) filesystemMagic(directory string) (uint32, error) {
	f.statfsCalls = append(f.statfsCalls, directory)
	metadata, exists := f.directories[directory]
	if !exists {
		return 0, unix.ENOENT
	}
	return metadata.magic, nil
}

func (f *fakeLinuxFilesystem) directoryCasefold(directory string) (bool, error) {
	f.casefoldCalls = append(f.casefoldCalls, directory)
	metadata := f.directories[directory]
	return metadata.casefold, metadata.casefoldErr
}

func (f *fakeLinuxFilesystem) directoryDevice(directory string) (linuxDevice, error) {
	metadata, exists := f.directories[directory]
	if !exists {
		return linuxDevice{}, unix.ENOENT
	}
	return metadata.device, metadata.deviceErr
}

func (f *fakeLinuxFilesystem) readMountInfo() ([]byte, error) {
	f.mountInfoReads++
	if f.mountInfoErr != nil {
		return nil, f.mountInfoErr
	}
	return []byte(f.mountInfo), nil
}

// directoryCaseは、このfilesystemを読むlinuxDirectoryCaseを返します。ttlが0なら判定とmountinfoを再利用しません。
func (f *fakeLinuxFilesystem) directoryCase(now func() time.Time, ttl time.Duration) *linuxDirectoryCase {
	return &linuxDirectoryCase{
		filesystemMagic: f.filesystemMagic,
		casefold:        f.directoryCasefold,
		device:          f.directoryDevice,
		mountInfo:       f.readMountInfo,
		now:             now,
		ttl:             ttl,
		limit:           linuxSensitiveVerdictLimit,
	}
}

var (
	// 試験用のFAT・exFATのdeviceです。
	fakeVFATDevice  = linuxDevice{major: 8, minor: 17}
	fakeExFATDevice = linuxDevice{major: 8, minor: 33}
)

// fakeMountInfoLineは、proc(5)の形式でmountinfoの一行を返します。
func fakeMountInfoLine(device linuxDevice, mountPoint, filesystemType, superOptions string) string {
	return "36 25 " + strconv.FormatUint(uint64(device.major), 10) + ":" + strconv.FormatUint(uint64(device.minor), 10) +
		" / " + mountPoint + " rw,relatime shared:1 - " + filesystemType + " /dev/sdb1 " + superOptions + "\n"
}

// fakeDefaultFATMountInfoは、大小文字を区別するoptionのないvfatとexFATのmountを表します。
var fakeDefaultFATMountInfo = "22 1 8:2 / / rw,relatime shared:1 - xfs /dev/sda2 rw,attr2,inode64,noquota\n" +
	fakeMountInfoLine(fakeVFATDevice, "/media/Usb", "vfat", "rw,fmask=0022,dmask=0022,codepage=437,iocharset=ascii,shortname=mixed,utf8,errors=remount-ro") +
	fakeMountInfoLine(fakeExFATDevice, "/media/Card", "exfat", "rw,fmask=0022,dmask=0022,iocharset=utf8,errors=remount-ro")

func countCalls(calls []string, directory string) int {
	count := 0
	for _, call := range calls {
		if call == directory {
			count++
		}
	}
	return count
}

// 読み取りで確かめられるcasefold属性と、大小文字を区別するmount optionのないFAT・exFATだけを、
// 大小文字を区別しないと判定します。mountinfoはFAT・exFATの場合だけ読みます。
func TestLinuxDirectoryCaseUsesFilesystemTypeAndCasefoldAttribute(t *testing.T) {
	const (
		// OpenZFS include/sys/fs/zfs.hのZFS_SUPER_MAGICです。
		zfsSuperMagic = 0x2fc12fc1
		// Linux fs/ntfs3のsuper blockのs_magic（"ntfs"）です。
		ntfs3SuperMagic = 0x7366746e
	)
	for _, test := range []struct {
		name           string
		directory      fakeLinuxDirectory
		wantSensitive  bool
		readsCasefold  bool
		readsMountInfo bool
	}{
		{name: "vfat", directory: fakeLinuxDirectory{magic: unix.MSDOS_SUPER_MAGIC, device: fakeVFATDevice}, readsMountInfo: true},
		{name: "exFAT", directory: fakeLinuxDirectory{magic: unix.EXFAT_SUPER_MAGIC, device: fakeExFATDevice}, readsMountInfo: true},
		{name: "ext4 casefold", directory: fakeLinuxDirectory{magic: unix.EXT4_SUPER_MAGIC, casefold: true}, readsCasefold: true},
		{name: "F2FS casefold", directory: fakeLinuxDirectory{magic: unix.F2FS_SUPER_MAGIC, casefold: true}, readsCasefold: true},
		{name: "tmpfs casefold", directory: fakeLinuxDirectory{magic: unix.TMPFS_MAGIC, casefold: true}, readsCasefold: true},
		{name: "ext4", directory: fakeLinuxDirectory{magic: unix.EXT4_SUPER_MAGIC}, wantSensitive: true, readsCasefold: true},
		{name: "F2FS", directory: fakeLinuxDirectory{magic: unix.F2FS_SUPER_MAGIC}, wantSensitive: true, readsCasefold: true},
		{name: "tmpfs", directory: fakeLinuxDirectory{magic: unix.TMPFS_MAGIC}, wantSensitive: true, readsCasefold: true},
		// casefold属性を読めない場合は、属性がないものとして扱います。
		{name: "ext4 ENOTTY", directory: fakeLinuxDirectory{magic: unix.EXT4_SUPER_MAGIC, casefold: true, casefoldErr: unix.ENOTTY}, wantSensitive: true, readsCasefold: true},
		{name: "ext4 EOPNOTSUPP", directory: fakeLinuxDirectory{magic: unix.EXT4_SUPER_MAGIC, casefold: true, casefoldErr: unix.EOPNOTSUPP}, wantSensitive: true, readsCasefold: true},
		{name: "F2FS EINVAL", directory: fakeLinuxDirectory{magic: unix.F2FS_SUPER_MAGIC, casefold: true, casefoldErr: unix.EINVAL}, wantSensitive: true, readsCasefold: true},
		{name: "tmpfs EACCES", directory: fakeLinuxDirectory{magic: unix.TMPFS_MAGIC, casefold: true, casefoldErr: unix.EACCES}, wantSensitive: true, readsCasefold: true},
		// serverやmount optionに依存するfilesystemは、属性を読まずに区別すると判定します。
		// casefold属性を返す設定にして、属性を参照しないことも確かめます。
		{name: "XFS", directory: fakeLinuxDirectory{magic: unix.XFS_SUPER_MAGIC, casefold: true}, wantSensitive: true},
		{name: "Btrfs", directory: fakeLinuxDirectory{magic: unix.BTRFS_SUPER_MAGIC, casefold: true}, wantSensitive: true},
		{name: "overlayfs", directory: fakeLinuxDirectory{magic: unix.OVERLAYFS_SUPER_MAGIC, casefold: true}, wantSensitive: true},
		{name: "SMB", directory: fakeLinuxDirectory{magic: unix.SMB_SUPER_MAGIC, casefold: true}, wantSensitive: true},
		{name: "SMB2", directory: fakeLinuxDirectory{magic: unix.SMB2_SUPER_MAGIC, casefold: true}, wantSensitive: true},
		{name: "CIFS", directory: fakeLinuxDirectory{magic: unix.CIFS_SUPER_MAGIC, casefold: true}, wantSensitive: true},
		{name: "NFS", directory: fakeLinuxDirectory{magic: unix.NFS_SUPER_MAGIC, casefold: true}, wantSensitive: true},
		{name: "FUSE", directory: fakeLinuxDirectory{magic: unix.FUSE_SUPER_MAGIC, casefold: true}, wantSensitive: true},
		{name: "9p", directory: fakeLinuxDirectory{magic: unix.V9FS_MAGIC, casefold: true}, wantSensitive: true},
		{name: "Ceph", directory: fakeLinuxDirectory{magic: unix.CEPH_SUPER_MAGIC, casefold: true}, wantSensitive: true},
		{name: "ZFS", directory: fakeLinuxDirectory{magic: zfsSuperMagic, casefold: true}, wantSensitive: true},
		{name: "ntfs3", directory: fakeLinuxDirectory{magic: ntfs3SuperMagic, casefold: true}, wantSensitive: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			const directory = "/mnt/Data"
			filesystem := &fakeLinuxFilesystem{
				directories: map[string]fakeLinuxDirectory{directory: test.directory},
				mountInfo:   fakeDefaultFATMountInfo,
			}
			sensitive, err := filesystem.directoryCase(time.Now, 0).caseSensitive(directory)
			if err != nil || sensitive != test.wantSensitive {
				t.Fatalf("caseSensitive(%q) = %t, %v; want %t, nil", directory, sensitive, err, test.wantSensitive)
			}
			if read := len(filesystem.casefoldCalls) != 0; read != test.readsCasefold {
				t.Fatalf("casefold attribute read = %t, want %t", read, test.readsCasefold)
			}
			if read := filesystem.mountInfoReads != 0; read != test.readsMountInfo {
				t.Fatalf("mountinfo read = %t, want %t", read, test.readsMountInfo)
			}
		})
	}

	t.Run("statfs failure", func(t *testing.T) {
		filesystem := &fakeLinuxFilesystem{}
		_, err := filesystem.directoryCase(time.Now, 0).caseSensitive("/mnt/Missing")
		if !errors.Is(err, unix.ENOENT) {
			t.Fatalf("caseSensitive(missing) error = %v, want ENOENT", err)
		}
		if len(filesystem.casefoldCalls) != 0 {
			t.Fatalf("casefold attribute was read after statfs failed: %q", filesystem.casefoldCalls)
		}
	})
}

// FAT・exFATのdirectoryは、st_devと同じmajor:minorのmountinfoの行に大小文字を区別するoptionが
// ないと確かめられた場合だけ、大小文字を区別しないと判定します。確かめられない場合は区別します。
func TestLinuxFATAndExFATCaseFollowsMountOptions(t *testing.T) {
	const vfatDefault = "rw,relatime,fmask=0022,dmask=0022,codepage=437,iocharset=ascii,shortname=mixed,utf8,errors=remount-ro"
	const exfatDefault = "rw,relatime,fmask=0022,dmask=0022,iocharset=utf8,errors=remount-ro"
	line := func(device linuxDevice, filesystemType, superOptions string) string {
		return fakeMountInfoLine(device, "/media/Usb", filesystemType, superOptions)
	}
	otherDevice := linuxDevice{major: 8, minor: 33}
	largeDevice := linuxDevice{major: 259, minor: 65537}
	for _, test := range []struct {
		name          string
		magic         uint32
		device        *linuxDevice
		deviceErr     error
		mountInfo     string
		mountInfoErr  error
		wantSensitive bool
	}{
		{name: "vfat default", magic: unix.MSDOS_SUPER_MAGIC, mountInfo: line(fakeVFATDevice, "vfat", vfatDefault)},
		{name: "vfat check=s", magic: unix.MSDOS_SUPER_MAGIC, mountInfo: line(fakeVFATDevice, "vfat", vfatDefault+",check=s"), wantSensitive: true},
		{name: "vfat check=r", magic: unix.MSDOS_SUPER_MAGIC, mountInfo: line(fakeVFATDevice, "vfat", vfatDefault+",check=r")},
		// vfatのnocaseはshortname=win95として扱われ、大小文字の扱いを変えません。
		{name: "vfat ignores nocase", magic: unix.MSDOS_SUPER_MAGIC, mountInfo: line(fakeVFATDevice, "vfat", vfatDefault+",nocase")},
		{name: "msdos default", magic: unix.MSDOS_SUPER_MAGIC, mountInfo: line(fakeVFATDevice, "msdos", "rw,fmask=0022,dmask=0022,codepage=437")},
		{name: "msdos nocase", magic: unix.MSDOS_SUPER_MAGIC, mountInfo: line(fakeVFATDevice, "msdos", "rw,fmask=0022,dmask=0022,codepage=437,nocase"), wantSensitive: true},
		// msdosのcheck=sは大文字を含む名前を拒否するため、大小文字だけが異なる名前は共存しません。
		{name: "msdos check=s", magic: unix.MSDOS_SUPER_MAGIC, mountInfo: line(fakeVFATDevice, "msdos", "rw,check=s")},
		{name: "exFAT", magic: unix.EXFAT_SUPER_MAGIC, mountInfo: line(fakeVFATDevice, "exfat", exfatDefault)},
		{name: "staging exFAT namecase=0", magic: unix.EXFAT_SUPER_MAGIC, mountInfo: line(fakeVFATDevice, "exfat", exfatDefault+",namecase=0")},
		{name: "staging exFAT namecase=1", magic: unix.EXFAT_SUPER_MAGIC, mountInfo: line(fakeVFATDevice, "exfat", exfatDefault+",namecase=1"), wantSensitive: true},
		{name: "no matching line", magic: unix.MSDOS_SUPER_MAGIC, mountInfo: line(otherDevice, "vfat", vfatDefault), wantSensitive: true},
		{name: "empty mountinfo", magic: unix.MSDOS_SUPER_MAGIC, wantSensitive: true},
		{name: "unreadable mountinfo", magic: unix.MSDOS_SUPER_MAGIC, mountInfoErr: os.ErrPermission, wantSensitive: true},
		{name: "stat failure", magic: unix.MSDOS_SUPER_MAGIC, deviceErr: unix.EACCES, mountInfo: line(fakeVFATDevice, "vfat", vfatDefault), wantSensitive: true},
		// 同じdeviceの複数の行（bind mount）は、全ての行で確かめられた扱いだけを使います。
		{name: "bind mounts", magic: unix.MSDOS_SUPER_MAGIC,
			mountInfo: line(fakeVFATDevice, "vfat", vfatDefault) + fakeMountInfoLine(fakeVFATDevice, "/srv/Usb", "vfat", vfatDefault)},
		{name: "bind mounts with check=s", magic: unix.MSDOS_SUPER_MAGIC, wantSensitive: true,
			mountInfo: line(fakeVFATDevice, "vfat", vfatDefault+",check=s") + fakeMountInfoLine(fakeVFATDevice, "/srv/Usb", "vfat", vfatDefault+",check=s")},
		{name: "conflicting lines", magic: unix.MSDOS_SUPER_MAGIC, wantSensitive: true,
			mountInfo: line(fakeVFATDevice, "vfat", vfatDefault) + fakeMountInfoLine(fakeVFATDevice, "/srv/Usb", "vfat", vfatDefault+",check=s")},
		{name: "conflicting lines with check=s first", magic: unix.MSDOS_SUPER_MAGIC, wantSensitive: true,
			mountInfo: line(fakeVFATDevice, "vfat", vfatDefault+",check=s") + fakeMountInfoLine(fakeVFATDevice, "/srv/Usb", "vfat", vfatDefault)},
		{name: "vfat line for exFAT type", magic: unix.EXFAT_SUPER_MAGIC, mountInfo: line(fakeVFATDevice, "vfat", vfatDefault), wantSensitive: true},
		{name: "exfat line for FAT type", magic: unix.MSDOS_SUPER_MAGIC, mountInfo: line(fakeVFATDevice, "exfat", exfatDefault), wantSensitive: true},
		{name: "other filesystem line", magic: unix.MSDOS_SUPER_MAGIC, mountInfo: line(fakeVFATDevice, "ext4", "rw,relatime"), wantSensitive: true},
		{name: "large device numbers", magic: unix.MSDOS_SUPER_MAGIC, device: &largeDevice, mountInfo: line(largeDevice, "vfat", vfatDefault)},
		// mount pointなどの8進数escape、任意の数のoptional field、空のmount sourceも読み分けます。
		{name: "escaped fields", magic: unix.MSDOS_SUPER_MAGIC,
			mountInfo: `41 25 8:17 /sub\040dir /media/My\040Usb\011tab\012line\134x rw,nosuid shared:12 master:3 propagate_from:4 unbindable - vfat /dev/disk/by-label/My\040Usb ` + vfatDefault + "\n"},
		{name: "escaped separator and empty source", magic: unix.MSDOS_SUPER_MAGIC, wantSensitive: true,
			mountInfo: `41 25 8:17 /\040-\040 /mnt/a\040-\040b rw - vfat  rw,check=s` + "\n"},
		{name: "empty source", magic: unix.MSDOS_SUPER_MAGIC, mountInfo: "41 25 8:17 / /media/Usb rw - vfat  " + vfatDefault + "\n"},
		{name: "malformed lines", magic: unix.MSDOS_SUPER_MAGIC,
			mountInfo: "garbage\n\n8:17 - vfat\n42 25 8:17 / /m rw\n43 25 x:y / /m rw - vfat /dev/sdb1 rw,check=s\n" + line(fakeVFATDevice, "vfat", vfatDefault)},
		{name: "last line without newline", magic: unix.MSDOS_SUPER_MAGIC, wantSensitive: true,
			mountInfo: strings.TrimSuffix(line(fakeVFATDevice, "vfat", vfatDefault+",check=s"), "\n")},
	} {
		t.Run(test.name, func(t *testing.T) {
			const directory = "/media/Usb"
			device := fakeVFATDevice
			if test.device != nil {
				device = *test.device
			}
			filesystem := &fakeLinuxFilesystem{
				directories:  map[string]fakeLinuxDirectory{directory: {magic: test.magic, device: device, deviceErr: test.deviceErr}},
				mountInfo:    test.mountInfo,
				mountInfoErr: test.mountInfoErr,
			}
			sensitive, err := filesystem.directoryCase(time.Now, 0).caseSensitive(directory)
			if err != nil || sensitive != test.wantSensitive {
				t.Fatalf("caseSensitive(%q) = %t, %v; want %t, nil", directory, sensitive, err, test.wantSensitive)
			}
		})
	}
}

// mountinfoの読み取りはttl以内だけ再利用し、記録にないdeviceでは読み直します。
// 区別しないという判定自体は再利用せず、毎回statfsから確かめます。
func TestLinuxMountInfoReadsAreReusedBriefly(t *testing.T) {
	strictDevice := linuxDevice{major: 8, minor: 49}
	filesystem := &fakeLinuxFilesystem{directories: map[string]fakeLinuxDirectory{
		"/media/Usb":    {magic: unix.MSDOS_SUPER_MAGIC, device: fakeVFATDevice},
		"/media/Card":   {magic: unix.EXFAT_SUPER_MAGIC, device: fakeExFATDevice},
		"/media/Strict": {magic: unix.MSDOS_SUPER_MAGIC, device: strictDevice},
	}, mountInfo: fakeMountInfoLine(fakeVFATDevice, "/media/Usb", "vfat", "rw")}
	now := time.Date(2026, 10, 22, 12, 0, 0, 0, time.UTC)
	directoryCase := filesystem.directoryCase(func() time.Time { return now }, time.Second)
	query := func(directory string, wantSensitive bool) {
		t.Helper()
		if sensitive, err := directoryCase.caseSensitive(directory); err != nil || sensitive != wantSensitive {
			t.Fatalf("caseSensitive(%q) = %t, %v; want %t", directory, sensitive, err, wantSensitive)
		}
	}
	reads := func(want int) {
		t.Helper()
		if filesystem.mountInfoReads != want {
			t.Fatalf("mountinfo reads = %d, want %d", filesystem.mountInfoReads, want)
		}
	}

	for range 3 {
		query("/media/Usb", false)
	}
	reads(1)
	if got := countCalls(filesystem.statfsCalls, "/media/Usb"); got != 3 {
		t.Fatalf("statfs calls for case-insensitive FAT = %d, want 3 (the verdict must not be reused)", got)
	}
	// 記録後に増えたmountは、ttl以内でも読み直して確かめます。
	filesystem.mountInfo += fakeMountInfoLine(fakeExFATDevice, "/media/Card", "exfat", "rw")
	query("/media/Card", false)
	reads(2)
	query("/media/Usb", false)
	reads(2)
	now = now.Add(time.Second)
	query("/media/Usb", false)
	reads(3)

	// 大小文字を区別するoptionのmountは、区別するという判定を再利用します。
	filesystem.mountInfo += fakeMountInfoLine(strictDevice, "/media/Strict", "vfat", "rw,check=s")
	query("/media/Strict", true)
	query("/media/Strict", true)
	reads(4)
	if got := countCalls(filesystem.statfsCalls, "/media/Strict"); got != 1 {
		t.Fatalf("statfs calls for case-sensitive FAT = %d, want 1", got)
	}

	// 同じdeviceを大小文字を区別するoptionでmountし直した場合は、ttl以内に読んだmountinfoで判定し、
	// 期限後に反映します。
	filesystem.mountInfo = fakeMountInfoLine(fakeVFATDevice, "/media/Usb", "vfat", "rw,check=s")
	query("/media/Usb", false)
	now = now.Add(time.Second)
	query("/media/Usb", true)

	// mountinfoを読めない場合は確かめられないため、大小文字を区別すると判定します。
	filesystem.mountInfoErr = os.ErrPermission
	now = now.Add(time.Second)
	query("/media/Card", true)

	// ttlが0ならmountinfoを再利用しません。
	uncached := &fakeLinuxFilesystem{
		directories: map[string]fakeLinuxDirectory{"/media/Usb": {magic: unix.MSDOS_SUPER_MAGIC, device: fakeVFATDevice}},
		mountInfo:   fakeMountInfoLine(fakeVFATDevice, "/media/Usb", "vfat", "rw"),
	}
	uncachedCase := uncached.directoryCase(time.Now, 0)
	for range 2 {
		if sensitive, err := uncachedCase.caseSensitive("/media/Usb"); err != nil || sensitive {
			t.Fatalf("caseSensitive without reuse = %t, %v; want case-insensitive", sensitive, err)
		}
	}
	if uncached.mountInfoReads != 2 {
		t.Fatalf("mountinfo reads without reuse = %d, want 2", uncached.mountInfoReads)
	}
}

// mountinfoから記録するFAT・exFATのdeviceは上限までで、超えたdeviceは確かめられないものとして扱います。
// 記録済みのdeviceの後続の行（bind mount）は、上限に達した後も反映します。
func TestParseLinuxMountCasesIsBounded(t *testing.T) {
	first, second, third := linuxDevice{major: 8, minor: 1}, linuxDevice{major: 8, minor: 2}, linuxDevice{major: 8, minor: 3}
	cases := parseLinuxMountCases(
		fakeMountInfoLine(first, "/a", "vfat", "rw")+
			fakeMountInfoLine(second, "/b", "exfat", "rw")+
			fakeMountInfoLine(third, "/c", "vfat", "rw")+
			fakeMountInfoLine(first, "/d", "vfat", "rw,check=s"),
		2)
	want := map[linuxDevice]linuxMountCase{first: linuxMountCaseUnconfirmed, second: linuxMountCaseExFATInsensitive}
	if len(cases) != len(want) {
		t.Fatalf("parseLinuxMountCases() = %v, want %v", cases, want)
	}
	for device, mountCase := range want {
		if got, exists := cases[device]; !exists || got != mountCase {
			t.Fatalf("parseLinuxMountCases()[%v] = %d, %t; want %d", device, got, exists, mountCase)
		}
	}
}

// directoryごとのcasefold属性とmountの境界を、ComparisonKeyとDirectoryNameCaseの共通の走査で確かめます。
func TestLinuxCaseKeyFollowsCasefoldDirectoriesAndMountBoundaries(t *testing.T) {
	strictDevice := linuxDevice{major: 8, minor: 49}
	filesystem := &fakeLinuxFilesystem{directories: map[string]fakeLinuxDirectory{
		"/":     {magic: unix.EXT4_SUPER_MAGIC},
		"/Data": {magic: unix.EXT4_SUPER_MAGIC, casefold: true},
		// 作成時に親から引き継いだ属性です。
		"/Data/Logs": {magic: unix.EXT4_SUPER_MAGIC, casefold: true},
		// 空のうちに属性を外したdirectoryです。
		"/Data/Logs/Plain": {magic: unix.EXT4_SUPER_MAGIC},
		// casefold directory配下のvfat mountと、その配下のNFS mountです。
		"/Data/Usb":       {magic: unix.MSDOS_SUPER_MAGIC, device: fakeVFATDevice},
		"/Data/Usb/Share": {magic: unix.NFS_SUPER_MAGIC},
		// 大小文字を区別するoption（check=s）でmountしたvfatです。
		"/Data/Strict": {magic: unix.MSDOS_SUPER_MAGIC, device: strictDevice},
		"/Scratch":     {magic: unix.TMPFS_MAGIC},
	}, mountInfo: fakeDefaultFATMountInfo + fakeMountInfoLine(strictDevice, "/Data/Strict", "vfat", "rw,check=s")}
	directoryCase := filesystem.directoryCase(time.Now, 0)
	for _, test := range []struct{ path, want string }{
		{path: "/Data/Logs/App.log", want: "/Data/logs/app.log"},
		{path: "/Data/Logs/Plain/App.log", want: "/Data/logs/plain/App.log"},
		{path: "/Data/Logs/Missing/Logs/App.log", want: "/Data/logs/missing/logs/app.log"},
		{path: "/Data/Logs/Plain/Missing/App.log", want: "/Data/logs/plain/Missing/App.log"},
		{path: "/Data/Usb/Logs/App.log", want: "/Data/usb/logs/app.log"},
		{path: "/Data/Usb/Share/Logs/App.log", want: "/Data/usb/share/Logs/App.log"},
		{path: "/Data/Strict/Logs/App.log", want: "/Data/strict/Logs/App.log"},
		{path: "/Scratch/Logs/App.log", want: "/Scratch/Logs/App.log"},
	} {
		if got := caseKey(test.path, directoryCase.caseSensitive); got != test.want {
			t.Errorf("caseKey(%q) = %q, want %q", test.path, got, test.want)
		}
	}
	for _, test := range []struct {
		directory string
		want      NameCase
	}{
		{directory: "/", want: NameCaseSensitive},
		{directory: "/Data", want: NameCaseInsensitive},
		{directory: "/Data/Logs", want: NameCaseInsensitive},
		{directory: "/Data/Logs/Missing", want: NameCaseInsensitive},
		{directory: "/Data/Logs/Plain", want: NameCaseSensitive},
		{directory: "/Data/Logs/Plain/Missing", want: NameCaseSensitive},
		{directory: "/Data/Usb", want: NameCaseInsensitive},
		{directory: "/Data/Usb/Share", want: NameCaseSensitive},
		{directory: "/Data/Usb/Share/Missing", want: NameCaseSensitive},
		{directory: "/Data/Strict", want: NameCaseSensitive},
		{directory: "/Data/Strict/Missing", want: NameCaseSensitive},
		{directory: "/Scratch", want: NameCaseSensitive},
	} {
		got := directoryNameCase(test.directory, directoryCase.caseSensitive)
		if got != test.want {
			t.Errorf("directoryNameCase(%q) = %d, want %d", test.directory, got, test.want)
		}
		entryKey := filepath.Base(caseKey(filepath.Join(test.directory, "Entry"), directoryCase.caseSensitive))
		if folded := entryKey == "entry"; folded != (got == NameCaseInsensitive) {
			t.Errorf("caseKey entry %q disagrees with directory rule %d for %q", entryKey, got, test.directory)
		}
	}
	// 問い合わせは操作用の表記を保ち、各階層の親だけを読みます。
	filesystem.statfsCalls = nil
	caseKey("/Data/Logs/App.log", directoryCase.caseSensitive)
	if want := []string{"/", "/Data", "/Data/Logs"}; !slices.Equal(filesystem.statfsCalls, want) {
		t.Fatalf("statfs queries = %q, want %q", filesystem.statfsCalls, want)
	}
}

// 大小文字を区別するという判定だけを短時間再利用し、区別しない判定と失敗は毎回確かめます。
func TestLinuxDirectoryCaseReusesOnlySensitiveVerdicts(t *testing.T) {
	filesystem := &fakeLinuxFilesystem{directories: map[string]fakeLinuxDirectory{
		"/xfs":      {magic: unix.XFS_SUPER_MAGIC},
		"/ext4":     {magic: unix.EXT4_SUPER_MAGIC},
		"/casefold": {magic: unix.EXT4_SUPER_MAGIC, casefold: true},
		"/vfat":     {magic: unix.MSDOS_SUPER_MAGIC, device: fakeVFATDevice},
		"/exfat":    {magic: unix.EXFAT_SUPER_MAGIC, device: fakeExFATDevice},
	}, mountInfo: fakeDefaultFATMountInfo}
	now := time.Date(2026, 10, 22, 12, 0, 0, 0, time.UTC)
	directoryCase := filesystem.directoryCase(func() time.Time { return now }, time.Second)
	query := func(directory string) (bool, error) {
		t.Helper()
		return directoryCase.caseSensitive(directory)
	}
	want := map[string]bool{"/xfs": true, "/ext4": true, "/casefold": false, "/vfat": false, "/exfat": false}
	for range 3 {
		for directory, sensitive := range want {
			if got, err := query(directory); err != nil || got != sensitive {
				t.Fatalf("caseSensitive(%q) = %t, %v; want %t", directory, got, err, sensitive)
			}
		}
		if _, err := query("/missing"); !errors.Is(err, unix.ENOENT) {
			t.Fatalf("caseSensitive(missing) error = %v, want ENOENT", err)
		}
	}
	for directory, wantStatfs := range map[string]int{
		"/xfs": 1, "/ext4": 1, "/casefold": 3, "/vfat": 3, "/exfat": 3, "/missing": 3,
	} {
		if got := countCalls(filesystem.statfsCalls, directory); got != wantStatfs {
			t.Errorf("statfs calls for %q = %d, want %d", directory, got, wantStatfs)
		}
	}
	if got := countCalls(filesystem.casefoldCalls, "/ext4"); got != 1 {
		t.Errorf("casefold reads for reused ext4 verdict = %d, want 1", got)
	}
	if got := countCalls(filesystem.casefoldCalls, "/casefold"); got != 3 {
		t.Errorf("casefold reads for casefold directory = %d, want 3", got)
	}
	// FAT・exFATの判定は毎回確かめますが、ttl以内はmountinfoの読み取りを再利用します。
	if filesystem.mountInfoReads != 1 {
		t.Errorf("mountinfo reads = %d, want 1", filesystem.mountInfoReads)
	}

	// 再利用中に区別しない状態へ変わっても、区別しないとは報告せず、期限後に検出します。
	newDevice := linuxDevice{major: 8, minor: 65}
	filesystem.directories["/xfs"] = fakeLinuxDirectory{magic: unix.MSDOS_SUPER_MAGIC, device: newDevice}
	filesystem.mountInfo += fakeMountInfoLine(newDevice, "/xfs", "vfat", "rw,fmask=0022")
	if got, err := query("/xfs"); err != nil || !got {
		t.Fatalf("reused verdict = %t, %v; want sensitive", got, err)
	}
	now = now.Add(time.Second)
	if got, err := query("/xfs"); err != nil || got {
		t.Fatalf("verdict after ttl = %t, %v; want newly mounted vfat to be insensitive", got, err)
	}
	// 区別する状態へ変わったdirectoryは、次の問い合わせで検出します。
	filesystem.directories["/vfat"] = fakeLinuxDirectory{magic: unix.XFS_SUPER_MAGIC}
	if got, err := query("/vfat"); err != nil || !got {
		t.Fatalf("verdict after unmounting vfat = %t, %v; want sensitive", got, err)
	}
	// 時刻が戻った場合は、記録した判定を再利用しません。
	statfsBefore := countCalls(filesystem.statfsCalls, "/ext4")
	now = now.Add(-time.Minute)
	if got, err := query("/ext4"); err != nil || !got {
		t.Fatalf("caseSensitive(ext4) = %t, %v; want sensitive", got, err)
	}
	if got := countCalls(filesystem.statfsCalls, "/ext4"); got != statfsBefore+1 {
		t.Fatalf("verdict recorded in the future was reused: statfs calls %d -> %d", statfsBefore, got)
	}

	// ttlが0なら判定を再利用しません。
	uncached := &fakeLinuxFilesystem{directories: map[string]fakeLinuxDirectory{"/xfs": {magic: unix.XFS_SUPER_MAGIC}}}
	for range 2 {
		if _, err := uncached.directoryCase(time.Now, 0).caseSensitive("/xfs"); err != nil {
			t.Fatal(err)
		}
	}
	if got := countCalls(uncached.statfsCalls, "/xfs"); got != 2 {
		t.Fatalf("statfs calls without reuse = %d, want 2", got)
	}
}

// 再利用する判定は件数に上限があり、期限切れの判定から破棄します。
func TestLinuxDirectoryCaseBoundsReusedVerdicts(t *testing.T) {
	filesystem := &fakeLinuxFilesystem{directories: map[string]fakeLinuxDirectory{
		"/a": {magic: unix.XFS_SUPER_MAGIC},
		"/b": {magic: unix.XFS_SUPER_MAGIC},
		"/c": {magic: unix.XFS_SUPER_MAGIC},
		"/d": {magic: unix.XFS_SUPER_MAGIC},
	}}
	start := time.Date(2026, 10, 22, 12, 0, 0, 0, time.UTC)
	now := start
	directoryCase := filesystem.directoryCase(func() time.Time { return now }, time.Second)
	directoryCase.limit = 2
	query := func(directory string) {
		t.Helper()
		if sensitive, err := directoryCase.caseSensitive(directory); err != nil || !sensitive {
			t.Fatalf("caseSensitive(%q) = %t, %v; want sensitive", directory, sensitive, err)
		}
	}
	statfs := func(directory string) int { return countCalls(filesystem.statfsCalls, directory) }

	query("/a")
	now = start.Add(500 * time.Millisecond)
	query("/b")
	// "/a"だけが期限切れなので、"/c"の記録では"/a"を破棄し、"/b"は再利用します。
	now = start.Add(1200 * time.Millisecond)
	query("/c")
	query("/b")
	query("/c")
	if statfs("/b") != 1 || statfs("/c") != 1 {
		t.Fatalf("fresh verdicts were not reused: b=%d c=%d", statfs("/b"), statfs("/c"))
	}
	// 期限内の判定だけで上限に達している場合は、全件を破棄してから記録します。
	query("/d")
	query("/d")
	query("/b")
	if statfs("/d") != 1 || statfs("/b") != 2 {
		t.Fatalf("full verdict set was not replaced: d=%d b=%d", statfs("/d"), statfs("/b"))
	}
}

// 複数goroutineから同時に問い合わせ、記録の上書き・期限切れ・上限での破棄が重なっても、
// 判定が変わらないことを確かめます。
func TestLinuxDirectoryCaseConcurrentQueriesKeepVerdicts(t *testing.T) {
	var clock atomic.Int64
	start := time.Date(2026, 10, 22, 12, 0, 0, 0, time.UTC)
	directoryCase := &linuxDirectoryCase{
		filesystemMagic: func(directory string) (uint32, error) {
			switch {
			case strings.HasPrefix(directory, "/sensitive"):
				return unix.XFS_SUPER_MAGIC, nil
			case strings.HasPrefix(directory, "/casefold"):
				return unix.EXT4_SUPER_MAGIC, nil
			case strings.HasPrefix(directory, "/vfat"), strings.HasPrefix(directory, "/strict"):
				return unix.MSDOS_SUPER_MAGIC, nil
			default:
				return 0, unix.ENOENT
			}
		},
		casefold: func(directory string) (bool, error) { return strings.HasPrefix(directory, "/casefold"), nil },
		device: func(directory string) (linuxDevice, error) {
			if strings.HasPrefix(directory, "/strict") {
				return linuxDevice{major: 8, minor: 49}, nil
			}
			return fakeVFATDevice, nil
		},
		mountInfo: func() ([]byte, error) {
			return []byte(fakeDefaultFATMountInfo + fakeMountInfoLine(linuxDevice{major: 8, minor: 49}, "/strict", "vfat", "rw,check=s")), nil
		},
		// 問い合わせごとに時刻を進め、期限切れと上限での破棄を起こします。
		now:   func() time.Time { return start.Add(time.Duration(clock.Add(1)) * time.Millisecond) },
		ttl:   20 * time.Millisecond,
		limit: 4,
	}
	var group sync.WaitGroup
	for worker := range 8 {
		group.Go(func() {
			for iteration := range 500 {
				index := strconv.Itoa((worker + iteration) % 10)
				for prefix, wantSensitive := range map[string]bool{"/sensitive": true, "/casefold": false, "/vfat": false, "/strict": true} {
					if sensitive, err := directoryCase.caseSensitive(prefix + index); err != nil || sensitive != wantSensitive {
						t.Errorf("caseSensitive(%q) = %t, %v; want %t", prefix+index, sensitive, err, wantSensitive)
						return
					}
				}
				if _, err := directoryCase.caseSensitive("/missing" + index); !errors.Is(err, unix.ENOENT) {
					t.Errorf("caseSensitive(missing) error = %v, want ENOENT", err)
					return
				}
			}
		})
	}
	group.Wait()
}

// useLinuxDirectoryCaseForTestは、filesystemCaseSensitiveの判定を差し替え、試験の終了時に戻します。
// process全体の設定なので、t.Parallelを使う試験からは呼び出さないでください。
func useLinuxDirectoryCaseForTest(t testing.TB, directoryCase *linuxDirectoryCase) {
	t.Helper()
	previous := filesystemDirectoryCase
	filesystemDirectoryCase = directoryCase
	t.Cleanup(func() { filesystemDirectoryCase = previous })
}

func pathWithinForTest(root, path string) bool {
	relative, err := filepath.Rel(root, path)
	return err == nil && relative != ".." && !strings.HasPrefix(relative, ".."+string(filepath.Separator))
}

// 公開APIが、差し替えたfilesystemの判定に従い、大小文字を区別する場合・しない場合の両方で
// 一貫したidentityを返すことを確かめます。実際のdirectoryは作成済みのものだけを使います。
func TestLinuxIdentityFollowsDirectoryMetadata(t *testing.T) {
	root := t.TempDir()
	casefold := filepath.Join(root, "Casefold")
	plain := filepath.Join(root, "Plain")
	usb := filepath.Join(root, "Usb")
	for _, directory := range []string{casefold, plain, usb} {
		if err := os.Mkdir(directory, 0o700); err != nil {
			t.Fatal(err)
		}
	}
	useLinuxDirectoryCaseForTest(t, &linuxDirectoryCase{
		filesystemMagic: func(directory string) (uint32, error) {
			if _, err := os.Stat(directory); err != nil {
				return 0, err
			}
			if pathWithinForTest(usb, directory) {
				return unix.MSDOS_SUPER_MAGIC, nil
			}
			return unix.EXT4_SUPER_MAGIC, nil
		},
		casefold: func(directory string) (bool, error) { return pathWithinForTest(casefold, directory), nil },
		device: func(directory string) (linuxDevice, error) {
			if pathWithinForTest(usb, directory) {
				return fakeVFATDevice, nil
			}
			return linuxDevice{major: 8, minor: 2}, nil
		},
		mountInfo: func() ([]byte, error) { return []byte(fakeDefaultFATMountInfo), nil },
		now:       time.Now,
	})

	for _, test := range []struct {
		directory string
		want      NameCase
	}{
		{directory: root, want: NameCaseSensitive},
		{directory: casefold, want: NameCaseInsensitive},
		{directory: filepath.Join(casefold, "Missing", "Logs"), want: NameCaseInsensitive},
		{directory: plain, want: NameCaseSensitive},
		{directory: usb, want: NameCaseInsensitive},
	} {
		if got := DirectoryNameCase(test.directory); got != test.want {
			t.Fatalf("DirectoryNameCase(%q) = %d, want %d", test.directory, got, test.want)
		}
	}
	for _, test := range []struct {
		directory string
		shared    bool
	}{
		{directory: casefold, shared: true},
		{directory: usb, shared: true},
		{directory: plain, shared: false},
	} {
		original := filepath.Join(test.directory, "Logs", "App.log")
		alias := filepath.Join(test.directory, "LOGS", "app.LOG")
		if same := ComparisonKey(original) == ComparisonKey(alias); same != test.shared {
			t.Errorf("ComparisonKey case alias under %q: same = %t, want %t", test.directory, same, test.shared)
		}
		if got := Contains(map[string]struct{}{original: {}}, alias); got != test.shared {
			t.Errorf("Contains(case alias under %q) = %t, want %t", test.directory, got, test.shared)
		}
		want := []string{original, alias}
		if test.shared {
			want = want[:1]
		}
		if got := AppendUniquePaths([]string{original, alias}); !slices.Equal(got, want) {
			t.Errorf("AppendUniquePaths under %q = %q, want %q", test.directory, got, want)
		}
		nameCase := DirectoryNameCase(test.directory)
		entry := filepath.Join(test.directory, "App.log")
		if got, want := EntryComparisonKey(entry, nameCase), ComparisonKey(entry); got != want {
			t.Errorf("EntryComparisonKey(%q, directory rule) = %q, want ComparisonKey %q", entry, got, want)
		}
	}
	// casefold directory自身の名前は、大小文字を区別する親の規則で比較します。
	if ComparisonKey(filepath.Join(casefold, "App.log")) == ComparisonKey(filepath.Join(root, "CASEFOLD", "App.log")) {
		t.Fatal("the casefold directory name was folded under a case-sensitive parent")
	}
	for _, path := range []string{filepath.Join(casefold, "Missing"), filepath.Join(casefold, "LOGS")} {
		if _, err := os.Lstat(path); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("identity inspection created %q: %v", path, err)
		}
	}
}

// 環境が用意している場合だけ、実際のcasefold directoryとFAT・exFATのmountで判定を確かめます。
// root権限は不要で、用意されていなければskipします。
//
// CIのtest (ubuntu-latest, case-insensitive TMPDIR)は、loop deviceでmountしたext4のcasefold directoryを
// 試験の一時directoryにし、vfat（runnerが対応していればexFATも）をmountして、両方がskipせずに実行されることを
// 確かめます。t.TempDirはGOTMPDIRを設定していればTMPDIRより優先するため、jobは試験processへ両方を渡します。
// ほかのjobや開発機では、casefoldに対応したkernelとfilesystem上の一時directory、FAT・exFATのmount（root権限が
// 必要）がなければskipします。判定の規則はstatfs・casefold属性・mountinfoを差し替えた上の試験で、実際のst_devと
// mountinfoの対応はTestStatDeviceMatchesMountInfoOfTemporaryDirectoryで、どの環境でも確かめます。
func TestLinuxDirectoryCaseDetectsAvailableCaseInsensitiveFilesystems(t *testing.T) {
	t.Run("casefold directory", func(t *testing.T) {
		directory := filepath.Join(t.TempDir(), "Casefold")
		if err := os.Mkdir(directory, 0o700); err != nil {
			t.Fatal(err)
		}
		if err := setCasefoldAttributeForTest(directory); err != nil {
			t.Skipf("casefold directory is unavailable on the temporary filesystem (%v); "+
				"set TMPDIR to an ext4, F2FS or tmpfs directory that supports casefold, as the case-insensitive TMPDIR CI job does "+
				"(t.TempDir uses GOTMPDIR instead when it is set)", err)
		}
		// 判定対象はext4・F2FS・tmpfsのcasefoldです。それ以外のfilesystemのcasefoldは区別すると判定します。
		if magic, err := statfsMagic(directory); err != nil || linuxFilesystemCaseForMagic(magic) != linuxFilesystemCaseByDirectory {
			t.Skipf("casefold on filesystem type %#x is outside the detected filesystems: %v", magic, err)
		}
		// filesystem自体が大小文字を区別しないことを、作成したfileの別表記で確かめます。
		original := filepath.Join(directory, "Probe.log")
		if err := os.WriteFile(original, nil, 0o600); err != nil {
			t.Fatal(err)
		}
		originalInfo, err := os.Stat(original)
		if err != nil {
			t.Fatal(err)
		}
		aliasInfo, err := os.Stat(filepath.Join(directory, "PROBE.LOG"))
		if err != nil || !os.SameFile(originalInfo, aliasInfo) {
			t.Fatalf("casefold directory did not resolve a case alias: %v", err)
		}
		child := filepath.Join(directory, "Child")
		if err := os.Mkdir(child, 0o700); err != nil {
			t.Fatal(err)
		}
		for _, current := range []string{directory, child, filepath.Join(directory, "Missing")} {
			if got := DirectoryNameCase(current); got != NameCaseInsensitive {
				t.Fatalf("DirectoryNameCase(%q) = %d, want case-insensitive", current, got)
			}
			if ComparisonKey(filepath.Join(current, "App.log")) != ComparisonKey(filepath.Join(current, "APP.LOG")) {
				t.Fatalf("ComparisonKey did not fold names below %q", current)
			}
		}
		if !Contains(map[string]struct{}{original: {}}, filepath.Join(directory, "PROBE.LOG")) {
			t.Fatal("Contains did not match the case alias of an existing file")
		}
	})
	t.Run("FAT or exFAT mount", func(t *testing.T) {
		mounts, err := fatFamilyMountsForTest()
		if err != nil {
			t.Skipf("mount table is unavailable: %v", err)
		}
		checked := 0
		for _, mount := range mounts {
			if _, err := os.Stat(mount.mountPoint); err != nil {
				continue
			}
			checked++
			// 大小文字を区別するmount optionがある場合だけ、大小文字を区別すると判定します。
			want := NameCaseInsensitive
			if mount.caseSensitiveOption {
				want = NameCaseSensitive
			}
			if got := DirectoryNameCase(mount.mountPoint); got != want {
				t.Errorf("DirectoryNameCase(%q) on %s with case-sensitive option=%t = %d, want %d",
					mount.mountPoint, mount.filesystemType, mount.caseSensitiveOption, got, want)
			}
			folded := ComparisonKey(filepath.Join(mount.mountPoint, "Logs", "App.log")) ==
				ComparisonKey(filepath.Join(mount.mountPoint, "LOGS", "APP.LOG"))
			if folded != (want == NameCaseInsensitive) {
				t.Errorf("ComparisonKey below %q folded names = %t, want %t", mount.mountPoint, folded, want == NameCaseInsensitive)
			}
		}
		if checked == 0 {
			t.Skip("no accessible vfat, msdos, or exfat mount; " +
				"mounting one needs root, as the case-insensitive TMPDIR CI job does")
		}
	})
}

// casefold属性はdirectoryだけを開いて読みます。FIFOなどを開いて待つことはありません。
func TestDirectoryHasCasefoldAttributeOpensOnlyDirectories(t *testing.T) {
	root := t.TempDir()
	folded, err := directoryHasCasefoldAttribute(root)
	// FS_IOC_GETFLAGSに対応しないfilesystemでは、属性なしとして扱うerrorを返します。
	if err != nil && (folded || !errors.Is(err, unix.ENOTTY) && !errors.Is(err, unix.EOPNOTSUPP) && !errors.Is(err, unix.EINVAL)) {
		t.Fatalf("directoryHasCasefoldAttribute(%q) = %t, %v", root, folded, err)
	}
	file := filepath.Join(root, "App.log")
	if err := os.WriteFile(file, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	// 一時directoryはcasefold属性を持つことがあります（case-insensitive TMPDIRのCI job）。
	// 属性があると報告した場合は、作成したfileを別表記でも開けることを実物で確かめます。
	if folded {
		original, err := os.Stat(file)
		if err != nil {
			t.Fatal(err)
		}
		alias, err := os.Stat(filepath.Join(root, "APP.LOG"))
		if err != nil || !os.SameFile(original, alias) {
			t.Fatalf("temporary directory %q reported the casefold attribute but did not resolve a case alias: %v", root, err)
		}
	}
	if _, err := directoryHasCasefoldAttribute(file); !errors.Is(err, unix.ENOTDIR) {
		t.Fatalf("directoryHasCasefoldAttribute(%q) error = %v, want ENOTDIR", file, err)
	}
	t.Run("FIFO", func(t *testing.T) {
		fifo := filepath.Join(root, "Pipe")
		if err := unix.Mkfifo(fifo, 0o600); err != nil {
			t.Skipf("FIFO unsupported on the temporary filesystem: %v", err)
		}
		result := make(chan error, 1)
		go func() {
			_, err := directoryHasCasefoldAttribute(fifo)
			result <- err
		}()
		select {
		case err := <-result:
			if !errors.Is(err, unix.ENOTDIR) {
				t.Fatalf("directoryHasCasefoldAttribute(%q) error = %v, want ENOTDIR", fifo, err)
			}
		case <-time.After(10 * time.Second):
			// 書き込み側を開き、FIFOのopenで待っている読み取りを解放してから失敗にします。
			if writer, err := os.OpenFile(fifo, os.O_WRONLY|unix.O_NONBLOCK, 0); err == nil {
				_ = writer.Close()
			}
			t.Fatalf("directoryHasCasefoldAttribute(%q) blocked opening a FIFO instead of rejecting it", fifo)
		}
	})
}

// FAT・exFATのmount optionは、statのst_devと同じmajor:minorを持つmountinfoの行で確かめます。
// casefoldやFATのmountがない環境でも、一時directoryとそれを含むmountの実際の行で、st_devの変換と
// mountinfoの解析が同じdeviceを指すことを確かめます。読み取るのは一時directoryとmountinfoだけです。
func TestStatDeviceMatchesMountInfoOfTemporaryDirectory(t *testing.T) {
	directory, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	magic, err := statfsMagic(directory)
	if err != nil {
		t.Fatal(err)
	}
	// Btrfsはsubvolumeごとのst_devを返し、mountinfoのdeviceと一致しないことがあります。
	// FAT・exFATの照合には影響しないため、対象外とします。
	if magic == unix.BTRFS_SUPER_MAGIC {
		t.Skipf("btrfs reports per-subvolume st_dev values for %q; run with TMPDIR on another filesystem", directory)
	}
	device, err := statDevice(directory)
	if err != nil {
		t.Fatal(err)
	}
	content, err := readSelfMountInfo()
	if err != nil {
		t.Skipf("mount table is unavailable: %v", err)
	}
	line, mountPoint, found := mountInfoLineForPathForTest(string(content), directory)
	if !found {
		t.Fatalf("no /proc/self/mountinfo line contains %q", directory)
	}
	// productionの解析がFATの行として読むdeviceと比べます。filesystemの種類だけを置き換え、
	// optional fieldやmount source・optionは実際の行のまま渡します。
	mountFields, filesystemFields, _ := strings.Cut(line, " - ")
	_, rest, _ := strings.Cut(filesystemFields, " ")
	parsed, _, ok := parseLinuxMountInfoLine(mountFields + " - vfat " + rest)
	if !ok || parsed != device {
		t.Fatalf("st_dev of %q = %d:%d, but mountinfo of its mount %q has device %+v (parsed %t) in %q",
			directory, device.major, device.minor, mountPoint, parsed, ok, line)
	}
}

// mountInfoLineForPathForTestは、mountinfoの行のうち、pathを含む最も深いmount pointの行を返します。
// 同じmount pointの行が複数ある場合は、後から重ねてmountした行を選びます。
func mountInfoLineForPathForTest(mountInfo, path string) (line, mountPoint string, found bool) {
	for candidate := range strings.Lines(mountInfo) {
		candidate = strings.TrimSuffix(candidate, "\n")
		mountFields, _, separated := strings.Cut(candidate, " - ")
		fields := strings.Fields(mountFields)
		if !separated || len(fields) < 5 {
			continue
		}
		point := unescapeMountInfoForTest(fields[4])
		if !pathWithinForTest(point, path) || (found && len(point) < len(mountPoint)) {
			continue
		}
		line, mountPoint, found = candidate, point, true
	}
	return line, mountPoint, found
}

// process全体で使う判定は、大小文字を区別するという判定と読み取ったmountinfoを最大2秒だけ
// 再利用し（README）、期限を壁時計で判定します。試験で差し替える判定ではなく、本番の判定の設定を
// 確かめます。
func TestFilesystemDirectoryCaseUsesDocumentedReuseSettings(t *testing.T) {
	directoryCase := filesystemDirectoryCase
	if linuxSensitiveVerdictTTL != 2*time.Second {
		t.Fatalf("linuxSensitiveVerdictTTL = %s, want the documented 2s", linuxSensitiveVerdictTTL)
	}
	if directoryCase.ttl != linuxSensitiveVerdictTTL {
		t.Fatalf("filesystemDirectoryCase reuses verdicts for %s, want %s", directoryCase.ttl, linuxSensitiveVerdictTTL)
	}
	if directoryCase.limit != linuxSensitiveVerdictLimit || directoryCase.limit <= 0 {
		t.Fatalf("filesystemDirectoryCase.limit = %d, want %d", directoryCase.limit, linuxSensitiveVerdictLimit)
	}
	if directoryCase.now == nil {
		t.Fatal("filesystemDirectoryCase has no clock")
	}
	if skew := time.Since(directoryCase.now()); skew.Abs() > time.Minute {
		t.Fatalf("filesystemDirectoryCase clock differs from the wall clock by %s", skew)
	}
	if directoryCase.filesystemMagic == nil || directoryCase.casefold == nil ||
		directoryCase.device == nil || directoryCase.mountInfo == nil {
		t.Fatal("filesystemDirectoryCase is missing a filesystem query")
	}
}

// setCasefoldAttributeForTestは、空のdirectoryへcasefold属性を付けます。filesystemが
// casefoldに対応しない場合はerrorを返します。試験用の一時directoryだけに使います。
func setCasefoldAttributeForTest(directory string) error {
	fd, err := unix.Open(directory, unix.O_RDONLY|unix.O_DIRECTORY|unix.O_CLOEXEC, 0)
	if err != nil {
		return err
	}
	defer unix.Close(fd)
	flags, err := unix.IoctlGetUint32(fd, unix.FS_IOC_GETFLAGS)
	if err != nil {
		return err
	}
	if err := unix.IoctlSetPointerInt(fd, unix.FS_IOC_SETFLAGS, int(flags|fsCasefoldFlag)); err != nil {
		return err
	}
	if flags, err = unix.IoctlGetUint32(fd, unix.FS_IOC_GETFLAGS); err != nil {
		return err
	}
	if flags&fsCasefoldFlag == 0 {
		return errors.New("filesystem accepted but did not keep the casefold attribute")
	}
	return nil
}

// fatFamilyMountForTestは、mountinfoのvfat・msdos・exfatのmountと、大小文字を区別するoptionの有無です。
type fatFamilyMountForTest struct {
	mountPoint          string
	filesystemType      string
	caseSensitiveOption bool
}

// fatFamilyMountsForTestは、/proc/self/mountinfoからvfat・msdos・exfatのmountを返します。
// 大小文字を区別するoptionは、vfatのcheck=s、msdosのnocase、staging exFAT driverのnamecase=1などです。
func fatFamilyMountsForTest() ([]fatFamilyMountForTest, error) {
	file, err := os.Open("/proc/self/mountinfo")
	if err != nil {
		return nil, err
	}
	defer file.Close()
	var mounts []fatFamilyMountForTest
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		// 36 35 98:0 /root /mount/point rw,noatime master:1 - vfat /dev/sdb1 rw,...
		mountFields, filesystemFields, found := strings.Cut(scanner.Text(), " - ")
		fields := strings.Fields(mountFields)
		if !found || len(fields) < 5 {
			continue
		}
		filesystemType, rest, _ := strings.Cut(filesystemFields, " ")
		_, superOptions, _ := strings.Cut(rest, " ")
		options := strings.Split(superOptions, ",")
		mount := fatFamilyMountForTest{mountPoint: unescapeMountInfoForTest(fields[4]), filesystemType: filesystemType}
		switch filesystemType {
		case "vfat":
			mount.caseSensitiveOption = slices.Contains(options, "check=s")
		case "msdos":
			mount.caseSensitiveOption = slices.Contains(options, "nocase")
		case "exfat":
			mount.caseSensitiveOption = slices.ContainsFunc(options, func(option string) bool {
				return strings.HasPrefix(option, "namecase=") && option != "namecase=0"
			})
		default:
			continue
		}
		mounts = append(mounts, mount)
	}
	return mounts, scanner.Err()
}

// unescapeMountInfoForTestは、mountinfoが8進数で表す空白などの文字（\040など）を戻します。
func unescapeMountInfoForTest(field string) string {
	var unescaped strings.Builder
	for index := 0; index < len(field); index++ {
		if field[index] == '\\' && index+3 < len(field) {
			if value, err := strconv.ParseUint(field[index+1:index+4], 8, 8); err == nil {
				unescaped.WriteByte(byte(value))
				index += 3
				continue
			}
		}
		unescaped.WriteByte(field[index])
	}
	return unescaped.String()
}

// FAT・exFATのmount optionを確かめるときの、/proc/self/mountinfoの読み取りと解析の費用です。
// 読み取り結果はttl以内だけ再利用するため、FAT・exFATを問い合わせている間も最大でttlごとに一回です。
func BenchmarkLinuxMountInfoRefresh(b *testing.B) {
	if _, err := readSelfMountInfo(); err != nil {
		b.Skipf("mount table is unavailable: %v", err)
	}
	for b.Loop() {
		content, err := readSelfMountInfo()
		if err != nil {
			b.Fatal(err)
		}
		parseLinuxMountCases(string(content), linuxSensitiveVerdictLimit)
	}
}

// 判定を再利用しない場合の費用です。statfs・FS_IOC_GETFLAGSを毎回呼び出します。
func BenchmarkLinuxComparisonKeyWithoutVerdictReuse(b *testing.B) {
	existing := benchmarkExistingDirectory(b)
	useLinuxDirectoryCaseForTest(b, &linuxDirectoryCase{
		filesystemMagic: statfsMagic,
		casefold:        directoryHasCasefoldAttribute,
		device:          statDevice,
		mountInfo:       readSelfMountInfo,
		now:             time.Now,
	})
	for _, bench := range []struct{ name, path string }{
		{name: "existing parent", path: filepath.Join(existing, "App_20260914_1.log")},
		{name: "missing parents", path: filepath.Join(existing, "Missing", "Logs", "App_20260914_1.log")},
	} {
		b.Run(bench.name, func(b *testing.B) {
			for b.Loop() {
				ComparisonKey(bench.path)
			}
		})
	}
}
