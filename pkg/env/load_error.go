package env

import "fmt"

// EnvFileError reports that an env file could not be read. Path is the file and
// Err the cause; errors.Is(err, fs.ErrNotExist) reports a missing file.
type EnvFileError struct {
	Path string
	Err  error
}

func (e *EnvFileError) Error() string {
	return fmt.Sprintf("load environment file %q: %v", e.Path, e.Err)
}

func (e *EnvFileError) Unwrap() error { return e.Err }

// InvalidEnvironmentValueError reports a value of an environment variable that
// a configuration loader read and then rejected because of what it means. Key
// names the variable, Value shows the value that was read, and Err is the
// sentinel for the kind of rejection or its cause. Value is empty for values
// that may contain credentials. A value that the functions of this package
// cannot convert to the expected type is reported as a VariableError instead,
// except for values that may contain credentials, which are reported as this
// error with only a sentinel, such as ErrInvalidJSON, in Err. Loaders join
// several errors with errors.Join, so use errors.As to find it.
type InvalidEnvironmentValueError struct {
	Key   string
	Value string
	Err   error

	// messageは、Errorが返す文面です。空の場合は、Key・Value・Errから組み立てます。
	message string
}

// NewInvalidEnvironmentValueError returns an InvalidEnvironmentValueError for the
// variable key whose Error method returns message. An empty message uses a
// default text built from key, value and err.
func NewInvalidEnvironmentValueError(key, value string, err error, message string) *InvalidEnvironmentValueError {
	return &InvalidEnvironmentValueError{Key: key, Value: value, Err: err, message: message}
}

func (e *InvalidEnvironmentValueError) Error() string {
	if e.message != "" {
		return e.message
	}
	return fmt.Sprintf("invalid %s %s: %v", e.Key, e.Value, e.Err)
}

func (e *InvalidEnvironmentValueError) Unwrap() error { return e.Err }

// RenamedEnvironmentKeyError reports that a variable whose name was corrected is
// still set under its old name in the environment or an env file. Old is the
// name that is set and New the current name. The old name is not read as an
// alias of the new one, and it is reported instead of being ignored, because
// ignoring it would silently drop a setting. Loaders join several of these
// errors with errors.Join, so use errors.As to find them.
type RenamedEnvironmentKeyError struct {
	Old string
	New string
}

func (e *RenamedEnvironmentKeyError) Error() string {
	return fmt.Sprintf(
		"environment variable %q was renamed to %q; rename it in the environment or env file",
		e.Old,
		e.New,
	)
}
