package main

import (
	"errors"
	"flag"
	"fmt"
	"io"
	"os"

	"github.com/sorotrail/sorotrail/internal/buildinfo"
)

// runVersion implements `sorotrail version`: it prints the build-time
// version, commit, and build date and exits 0. The same output is available
// via the top-level `sorotrail --version` / `sorotrail -V` flags, so scripts
// that probe the binary without a subcommand (CI, bug reports) share one path.
func runVersion(args []string) error {
	return runVersionTo(os.Stdout, args)
}

// runVersionTo is runVersion with an explicit output writer so tests can
// assert on the output without touching stdout.
func runVersionTo(w io.Writer, args []string) error {
	fs := flag.NewFlagSet("version", flag.ContinueOnError)
	fs.Usage = func() {
		fmt.Fprint(fs.Output(), `usage: sorotrail version

Prints the build version, commit, and build date, one per line, and
exits 0. The same output is available from the top-level flags
--version and -V.
`)
	}
	if err := fs.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return nil // usage already printed
		}
		return err
	}
	if len(fs.Args()) != 0 {
		fs.Usage()
		return errors.New("version takes no positional arguments")
	}
	_, err := fmt.Fprint(w, formatVersion(currentVersion()))
	return err
}

// versionInfo bundles the build-time values reported by `sorotrail version`.
type versionInfo struct {
	Version   string
	Commit    string
	BuildDate string
}

// currentVersion reads the build-time values injected via -ldflags.
// Without ldflags (plain `go build`, unit tests) every field is "unknown".
func currentVersion() versionInfo {
	return versionInfo{
		Version:   buildinfo.Version,
		Commit:    buildinfo.Commit,
		BuildDate: buildinfo.BuildDate,
	}
}

// formatVersion renders the version report. The layout is stable so
// parsing scripts (and tests) can rely on one value per line.
func formatVersion(v versionInfo) string {
	return fmt.Sprintf("version\t%s\ncommit\t%s\nbuild-date\t%s\n",
		v.Version, v.Commit, v.BuildDate)
}
