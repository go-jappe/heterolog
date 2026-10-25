//go:build linux

package pathidentity

import (
	"errors"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"golang.org/x/sys/unix"
)

// Linuxでは、読み取りだけで確かめられる次のdirectoryを、大小文字を区別しないと判定します。
//   - casefold属性（FS_CASEFOLD_FL）を持つdirectory。ext4・F2FS・tmpfsのcasefoldです。
//     属性は作成時に親から引き継がれますが、directoryごとに異なる場合もあります。
//   - FAT（vfat・msdos）とexFATのfilesystem上のdirectory。ただし、大小文字を区別するmount option
//     （vfatのcheck=s、msdosのnocase、Linux 5.4〜5.6のstaging exFAT driverのnamecase=1）がないことを
//     /proc/self/mountinfoで確かめられた場合だけです。
//
// SMB/CIFS・NFS・NTFS（ntfs3・ntfs-3g）・FUSE（exfat-fuseなどのfuseblkを含む）・9p・ZFSなどは、
// serverやmount optionで挙動が変わり外から確かめられないため、大小文字を区別すると判定します。
// 判定はstatfs・stat・FS_IOC_GETFLAGS・mountinfoの読み取りだけで行い、probe用のfileやdirectoryは作成しません。

// Linux FS_CASEFOLD_FL (Linux 5.2+) is declared in the public UAPI include/uapi/linux/fs.h:
// https://github.com/torvalds/linux/blob/master/include/uapi/linux/fs.h
// golang.org/x/sys/unix does not define it.
const fsCasefoldFlag = 0x40000000

// linuxSensitiveVerdictTTLは、大小文字を区別すると判定したdirectoryの結果と、読み取った
// mountinfoを再利用する期間です。SMB/CIFS・NFS・FUSEなどのstatfsはserverやdaemonへ問い合わせるため、
// 走査のたびに同じ問い合わせを繰り返さないようにします。区別しないという判定自体は再利用しないため、
// casefold属性の解除やmountの取り外しは次の問い合わせで反映します。新しいmountやcasefold属性の付与、
// 同じdeviceを別のmount optionでmountし直した場合は、最大でこの期間だけ遅れて反映します。
const linuxSensitiveVerdictTTL = 2 * time.Second

// linuxSensitiveVerdictLimitは、再利用する判定と、mountinfoから記録するFAT・exFATのdeviceの最大件数です。
const linuxSensitiveVerdictLimit = 4096

// linuxFilesystemCaseは、statfsのf_typeから分かるfilesystemの大小文字の扱いです。
type linuxFilesystemCase uint8

const (
	// linuxFilesystemCaseSensitiveは、大小文字を区別すると扱うfilesystemです。
	linuxFilesystemCaseSensitive linuxFilesystemCase = iota
	// linuxFilesystemCaseByMountOptionsは、大小文字を区別するmount optionがなければ区別しないfilesystemです。
	linuxFilesystemCaseByMountOptions
	// linuxFilesystemCaseByDirectoryは、directoryのcasefold属性で決まるfilesystemです。
	linuxFilesystemCaseByDirectory
)

// linuxFilesystemCaseForMagicは、statfsのf_typeからfilesystemの扱いを返します。
// ext2・ext3もext4と同じ値ですが、casefold属性を持たないため区別すると判定します。
func linuxFilesystemCaseForMagic(magic uint32) linuxFilesystemCase {
	switch magic {
	case unix.MSDOS_SUPER_MAGIC, unix.EXFAT_SUPER_MAGIC:
		return linuxFilesystemCaseByMountOptions
	case unix.EXT4_SUPER_MAGIC, unix.F2FS_SUPER_MAGIC, unix.TMPFS_MAGIC:
		return linuxFilesystemCaseByDirectory
	default:
		return linuxFilesystemCaseSensitive
	}
}

// linuxDeviceは、statのst_devやmountinfoが示すdeviceのmajor・minor番号です。
type linuxDevice struct {
	major, minor uint32
}

// linuxMountCaseは、mountinfoの行から確かめたFAT・exFATのmountの大小文字の扱いです。
type linuxMountCase uint8

const (
	// linuxMountCaseUnconfirmedは、大小文字を区別しないと確かめられないmountです。
	linuxMountCaseUnconfirmed linuxMountCase = iota
	// linuxMountCaseFATInsensitiveは、大小文字を区別するoptionのないvfat・msdosのmountです。
	linuxMountCaseFATInsensitive
	// linuxMountCaseExFATInsensitiveは、大小文字を区別するoptionのないexFATのmountです。
	linuxMountCaseExFATInsensitive
)

// linuxMountSnapshotは、mountinfoから読み取ったFAT・exFATのdeviceごとの扱いと、その読み取り時刻です。
type linuxMountSnapshot struct {
	readAt time.Time
	cases  map[linuxDevice]linuxMountCase
}

// linuxDirectoryCaseは、directoryのfilesystemの種類・casefold属性・mount optionから大小文字の規則を
// 判定します。filesystemMagicはstatfsのf_type、casefoldはcasefold属性の有無、deviceはstatのst_dev、
// mountInfoは/proc/self/mountinfoの内容を返します。試験ではこれらと時刻を差し替えます。
// 複数goroutineから並行して使えます。
type linuxDirectoryCase struct {
	filesystemMagic func(directory string) (uint32, error)
	casefold        func(directory string) (bool, error)
	device          func(directory string) (linuxDevice, error)
	mountInfo       func() ([]byte, error)
	now             func() time.Time
	ttl             time.Duration
	limit           int

	mu sync.RWMutex
	// sensitiveは、大小文字を区別すると判定したdirectoryと、その判定時刻です。
	sensitive map[string]time.Time

	mountsMu sync.Mutex
	// mountsは、最後に読み取ったmountinfoです。読み取れなかった場合はnilです。
	mounts *linuxMountSnapshot
}

// filesystemDirectoryCaseは、filesystemCaseSensitiveが使う判定です。
var filesystemDirectoryCase = &linuxDirectoryCase{
	filesystemMagic: statfsMagic,
	casefold:        directoryHasCasefoldAttribute,
	device:          statDevice,
	mountInfo:       readSelfMountInfo,
	now:             time.Now,
	ttl:             linuxSensitiveVerdictTTL,
	limit:           linuxSensitiveVerdictLimit,
}

// caseSensitiveは、directory直下の名前が大小文字を区別するかを返します。
// statfsに失敗した場合（未作成のdirectoryなど）はerrorを返し、呼び出し元は親の規則を引き継ぎます。
// casefold属性を読めない場合（ENOTTY・EOPNOTSUPP・EINVAL・EACCESなど）は、属性なしとして扱います。
func (d *linuxDirectoryCase) caseSensitive(directory string) (bool, error) {
	return d.caseSensitiveAt(directory, d.now())
}

// walkCaseSensitiveは、一回の走査の間、判定時刻を走査の開始時刻に固定したcaseSensitiveを返します。
func (d *linuxDirectoryCase) walkCaseSensitive() func(string) (bool, error) {
	now := d.now()
	return func(directory string) (bool, error) { return d.caseSensitiveAt(directory, now) }
}

func (d *linuxDirectoryCase) caseSensitiveAt(directory string, now time.Time) (bool, error) {
	// 判定時刻は壁時計で比べ、時刻が戻った場合は再利用しません。
	now = now.Round(0)
	if d.recentlySensitive(directory, now) {
		return true, nil
	}
	magic, err := d.filesystemMagic(directory)
	if err != nil {
		return false, err
	}
	switch linuxFilesystemCaseForMagic(magic) {
	case linuxFilesystemCaseByMountOptions:
		if d.mountConfirmsInsensitive(directory, magic, now) {
			return false, nil
		}
	case linuxFilesystemCaseByDirectory:
		if folded, err := d.casefold(directory); err == nil && folded {
			return false, nil
		}
	}
	d.rememberSensitive(directory, now)
	return true, nil
}

// mountConfirmsInsensitiveは、FAT・exFATのdirectoryのmountに大小文字を区別するoptionがないことを、
// st_devと同じmajor:minorのmountinfoの行で確かめられたかを返します。stat・mountinfoを読めない場合や、
// 対応する行がない場合、行のfilesystemの種類がf_typeと合わない場合は確かめられません。
func (d *linuxDirectoryCase) mountConfirmsInsensitive(directory string, magic uint32, now time.Time) bool {
	device, err := d.device(directory)
	if err != nil {
		return false
	}
	switch d.mountCase(device, now) {
	case linuxMountCaseFATInsensitive:
		return magic == unix.MSDOS_SUPER_MAGIC
	case linuxMountCaseExFATInsensitive:
		return magic == unix.EXFAT_SUPER_MAGIC
	default:
		return false
	}
}

// mountCaseは、deviceのmountの扱いを返します。ttl以内に読み取ったmountinfoを再利用し、
// 期限切れの場合や、記録にないdevice（読み取り後のmountなど）の場合は読み直します。
func (d *linuxDirectoryCase) mountCase(device linuxDevice, now time.Time) linuxMountCase {
	d.mountsMu.Lock()
	defer d.mountsMu.Unlock()
	if d.mounts != nil && d.fresh(d.mounts.readAt, now) {
		if mountCase, exists := d.mounts.cases[device]; exists {
			return mountCase
		}
	}
	content, err := d.mountInfo()
	if err != nil {
		d.mounts = nil
		return linuxMountCaseUnconfirmed
	}
	d.mounts = &linuxMountSnapshot{readAt: now, cases: parseLinuxMountCases(string(content), linuxSensitiveVerdictLimit)}
	return d.mounts.cases[device]
}

// Linux fs/fat/inode.c fat_show_options prints the case options of vfat and msdos as
//
//	if (opts->name_check != 'n')
//		seq_printf(m, ",check=%c", opts->name_check);
//	if (!isvfat) {
//		...
//		if (opts->nocase)
//			seq_puts(m, ",nocase");
//
// https://github.com/torvalds/linux/blob/master/fs/fat/inode.c
// vfat with check=s compares names with memcmp (fat_name_match, vfat_dentry_ops), and msdos with
// nocase keeps lowercase letters (msdos_format_name). msdos with check=s rejects uppercase letters,
// so names differing only in case cannot coexist. The staging exFAT driver of Linux 5.4-5.6 prints
// ",namecase=%u" (exfat_show_options), and a nonzero value makes nls_upper keep the case:
// https://github.com/torvalds/linux/blob/v5.6/drivers/staging/exfat/exfat_super.c
// fs/exfat (Linux 5.7+) accepts namecase as a deprecated option and ignores it.

// parseLinuxMountCasesは、/proc/self/mountinfoの内容から、vfat・msdos・exfatの行があるdeviceごとの
// 扱いを返します。同じdeviceの行が複数ある場合（bind mountなど）は、全ての行で同じ扱いを確かめられた
// ときだけ採用します。記録するdeviceはlimit件までで、超えたdeviceは確かめられないものとして扱います。
func parseLinuxMountCases(mountInfo string, limit int) map[linuxDevice]linuxMountCase {
	cases := make(map[linuxDevice]linuxMountCase)
	for line := range strings.Lines(mountInfo) {
		device, mountCase, found := parseLinuxMountInfoLine(strings.TrimSuffix(line, "\n"))
		if !found {
			continue
		}
		previous, exists := cases[device]
		if !exists && len(cases) >= limit {
			continue
		}
		if exists && previous != mountCase {
			mountCase = linuxMountCaseUnconfirmed
		}
		cases[device] = mountCase
	}
	return cases
}

// parseLinuxMountInfoLineは、mountinfoの一行がvfat・msdos・exfatのmountなら、そのdeviceと扱いを返します。
// 形式はproc(5)の「36 35 98:0 /root /mount/point rw,noatime master:1 - vfat /dev/sdb1 rw,check=s」です。
// mount pointなどの空白は8進数のescape（\040など）で表されるため、区切りの" - "や空白とは衝突しません。
// mount sourceは空の場合もあるため、後半は一つずつの空白で分けます。
func parseLinuxMountInfoLine(line string) (linuxDevice, linuxMountCase, bool) {
	mountFields, filesystemFields, found := strings.Cut(line, " - ")
	fields := strings.Fields(mountFields)
	if !found || len(fields) < 6 {
		return linuxDevice{}, linuxMountCaseUnconfirmed, false
	}
	majorText, minorText, found := strings.Cut(fields[2], ":")
	major, majorErr := strconv.ParseUint(majorText, 10, 32)
	minor, minorErr := strconv.ParseUint(minorText, 10, 32)
	if !found || majorErr != nil || minorErr != nil {
		return linuxDevice{}, linuxMountCaseUnconfirmed, false
	}
	device := linuxDevice{major: uint32(major), minor: uint32(minor)}
	filesystemType, rest, _ := strings.Cut(filesystemFields, " ")
	_, superOptions, _ := strings.Cut(rest, " ")
	options := strings.Split(superOptions, ",")
	switch filesystemType {
	case "vfat":
		if slices.Contains(options, "check=s") {
			return device, linuxMountCaseUnconfirmed, true
		}
		return device, linuxMountCaseFATInsensitive, true
	case "msdos":
		if slices.Contains(options, "nocase") {
			return device, linuxMountCaseUnconfirmed, true
		}
		return device, linuxMountCaseFATInsensitive, true
	case "exfat":
		for _, option := range options {
			if value, isNamecase := strings.CutPrefix(option, "namecase="); isNamecase && value != "0" {
				return device, linuxMountCaseUnconfirmed, true
			}
		}
		return device, linuxMountCaseExFATInsensitive, true
	default:
		return linuxDevice{}, linuxMountCaseUnconfirmed, false
	}
}

// recentlySensitiveは、ttl以内に大小文字を区別すると判定したdirectoryかを返します。
// 期限切れの判定は、次の記録で上書きするか、件数が上限に達したときに破棄します。
func (d *linuxDirectoryCase) recentlySensitive(directory string, now time.Time) bool {
	d.mu.RLock()
	defer d.mu.RUnlock()
	checked, exists := d.sensitive[directory]
	return exists && d.fresh(checked, now)
}

// rememberSensitiveは、大小文字を区別するという判定を記録します。件数が上限に達した場合は、
// 期限切れの判定を除き、それでも上限なら全件を破棄します。
func (d *linuxDirectoryCase) rememberSensitive(directory string, now time.Time) {
	if d.ttl <= 0 || d.limit <= 0 {
		return
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.sensitive == nil {
		d.sensitive = make(map[string]time.Time)
	}
	if _, exists := d.sensitive[directory]; !exists && len(d.sensitive) >= d.limit {
		for cached, checked := range d.sensitive {
			if !d.fresh(checked, now) {
				delete(d.sensitive, cached)
			}
		}
		if len(d.sensitive) >= d.limit {
			clear(d.sensitive)
		}
	}
	d.sensitive[directory] = now
}

func (d *linuxDirectoryCase) fresh(checked, now time.Time) bool {
	age := now.Sub(checked)
	return age >= 0 && age < d.ttl
}

// statfsMagicは、directoryのfilesystemのmagic number（statfsのf_type）を返します。
func statfsMagic(directory string) (uint32, error) {
	var stat unix.Statfs_t
	if err := ignoringEINTR(func() error { return unix.Statfs(directory, &stat) }); err != nil {
		return 0, err
	}
	// f_typeの型はarchitectureで異なりますが、magic numberは32bitです。
	return uint32(stat.Type), nil
}

// statDeviceは、directoryのst_devをmajor・minor番号で返します。statfsと同じくlinkをたどります。
func statDevice(directory string) (linuxDevice, error) {
	var stat unix.Stat_t
	if err := ignoringEINTR(func() error { return unix.Stat(directory, &stat) }); err != nil {
		return linuxDevice{}, err
	}
	// st_devの型はarchitectureで異なります。
	dev := uint64(stat.Dev)
	return linuxDevice{major: unix.Major(dev), minor: unix.Minor(dev)}, nil
}

// readSelfMountInfoは、このprocessのmount namespaceの/proc/self/mountinfoを読み取ります。
func readSelfMountInfo() ([]byte, error) {
	return os.ReadFile("/proc/self/mountinfo")
}

// directoryHasCasefoldAttributeは、directoryがcasefold属性を持つかをFS_IOC_GETFLAGSで返します。
// O_DIRECTORYで開くため、directory以外のfileは開きません。
func directoryHasCasefoldAttribute(directory string) (bool, error) {
	var fd int
	err := ignoringEINTR(func() (err error) {
		fd, err = unix.Open(directory, unix.O_RDONLY|unix.O_DIRECTORY|unix.O_CLOEXEC, 0)
		return err
	})
	if err != nil {
		return false, err
	}
	defer unix.Close(fd)
	// FS_IOC_GETFLAGSの引数はlongと宣言されていますが、kernelはintを読み書きします（ioctl_iflags(2)）。
	var flags uint32
	err = ignoringEINTR(func() (err error) {
		flags, err = unix.IoctlGetUint32(fd, unix.FS_IOC_GETFLAGS)
		return err
	})
	if err != nil {
		return false, err
	}
	return flags&fsCasefoldFlag != 0, nil
}

func ignoringEINTR(operation func() error) error {
	for {
		if err := operation(); !errors.Is(err, unix.EINTR) {
			return err
		}
	}
}

// filesystemCaseSensitiveは、directory直下の名前が大小文字を区別するかを返します。
func filesystemCaseSensitive(directory string) (bool, error) {
	return filesystemDirectoryCase.caseSensitive(directory)
}

// filesystemPathKeyは、各階層の親directoryの判定で名前を揃えます。絶対化できなかったpathは、
// 問い合わせるdirectoryを特定できないため、従来どおり元の表記で比較します。
func filesystemPathKey(path string) string {
	if !filepath.IsAbs(path) {
		return path
	}
	return caseKey(path, filesystemDirectoryCase.walkCaseSensitive())
}

// filesystemDirectoryNameCaseはComparisonKeyと同じ問い合わせで、directory直下の規則を返します。
func filesystemDirectoryNameCase(directory string) NameCase {
	if !filepath.IsAbs(directory) {
		return NameCaseSensitive
	}
	return directoryNameCase(directory, filesystemDirectoryCase.walkCaseSensitive())
}
