package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"strings"
	"text/tabwriter"
	"time"

	"github.com/sorotrail/sorotrail/internal/workerhealth"
)

type checkSliceFlag []string

func (c *checkSliceFlag) String() string {
	return strings.Join(*c, ",")
}

func (c *checkSliceFlag) Set(val string) error {
	*c = append(*c, val)
	return nil
}

func runPreflight(args []string) error {
	return runPreflightTo(context.Background(), args, os.Stdout, workerhealth.PreflightOptions{
		WriteProbe: true,
	})
}

func runPreflightTo(ctx context.Context, args []string, out io.Writer, baseOpts workerhealth.PreflightOptions) error {
	fs := flag.NewFlagSet("sorotrail preflight", flag.ContinueOnError)
	fs.SetOutput(io.Discard)

	var (
		jsonFlag               bool
		format                 string
		timeout                time.Duration
		checkSpecs             checkSliceFlag
		writeProbe             = baseOpts.WriteProbe
		readOnly               bool
		allowPendingMigrations = baseOpts.AllowPendingMigrations
	)

	fs.BoolVar(&jsonFlag, "json", false, "output report as machine-readable JSON")
	fs.StringVar(&format, "format", "text", "output format: text or json")
	fs.DurationVar(&timeout, "timeout", 10*time.Second, "bounded timeout for dependency checks")
	fs.Var(&checkSpecs, "check", "comma-separated or repeatable check names to run (config,database,migrations,rpc,archive,retention)")
	fs.BoolVar(&writeProbe, "write-probe", writeProbe, "verify archive write permission by writing and deleting a temporary probe object")
	fs.BoolVar(&readOnly, "read-only", false, "disable temporary object write probe on archive storage")
	fs.BoolVar(&allowPendingMigrations, "allow-pending-migrations", allowPendingMigrations, "allow pending migrations if the database user has permission to apply them")

	fs.Usage = func() {
		fmt.Fprintln(os.Stderr, `usage: sorotrail preflight [flags]

Run startup preflight self-checks (configuration, database connectivity,
migration state and permissions, Soroban RPC reachability and network
passphrase match, object-storage archival access, and retention/archival
coherence) before deploying or starting the service.

Flags:
  --json                       Output results as JSON
  --format <text|json>         Output format (default: text)
  --timeout <duration>         Timeout for dependency probes (default: 10s)
  --check <names>              Subset of checks to run: config,database,migrations,rpc,archive,retention
  --write-probe                Verify S3 archive write/delete permission with a temporary object (default: true)
  --read-only                  Skip the temporary S3 write probe and perform read-only checks only
  --allow-pending-migrations   Pass the migrations check when migrations are pending if the DB user can apply them

Exit codes:
  0  All selected checks passed
  1  One or more dependency checks failed
  2  Configuration validation or CLI flag error`)
	}

	if err := fs.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			fs.Usage()
			return nil
		}
		return &exitError{code: exitConfigError, err: err}
	}
	if fs.NArg() > 0 {
		return &exitError{
			code: exitConfigError,
			err:  fmt.Errorf("unexpected positional arguments: %s", strings.Join(fs.Args(), " ")),
		}
	}

	if jsonFlag {
		format = "json"
	}
	format = strings.ToLower(strings.TrimSpace(format))
	if format != "text" && format != "json" {
		return &exitError{
			code: exitConfigError,
			err:  fmt.Errorf("unsupported --format %q (must be text or json)", format),
		}
	}
	if timeout <= 0 {
		return &exitError{
			code: exitConfigError,
			err:  fmt.Errorf("--timeout must be positive, got %s", timeout),
		}
	}

	selectedChecks, err := workerhealth.ParseCheckSelection(checkSpecs)
	if err != nil {
		return &exitError{code: exitConfigError, err: err}
	}

	if readOnly {
		writeProbe = false
	}

	opts := baseOpts
	opts.Checks = selectedChecks
	opts.Timeout = timeout
	opts.WriteProbe = writeProbe
	opts.AllowPendingMigrations = allowPendingMigrations

	report := workerhealth.RunPreflight(ctx, opts)

	if format == "json" {
		enc := json.NewEncoder(out)
		enc.SetIndent("", "  ")
		if err := enc.Encode(report); err != nil {
			return err
		}
	} else {
		renderPreflightText(out, report)
	}

	switch report.ExitCode {
	case workerhealth.ExitOK:
		return nil
	case workerhealth.ExitConfigError:
		return &exitError{
			code: exitConfigError,
			err:  fmt.Errorf("preflight configuration check failed"),
		}
	default:
		return &exitError{
			code: exitRuntimeError,
			err:  fmt.Errorf("preflight self-check failed"),
		}
	}
}

func renderPreflightText(out io.Writer, report workerhealth.PreflightReport) {
	tw := tabwriter.NewWriter(out, 0, 4, 2, ' ', 0)
	fmt.Fprintln(tw, "CHECK\tSTATUS\tLATENCY\tSUMMARY")
	for _, chk := range report.Checks {
		summary := chk.Detail
		if chk.Status == workerhealth.StatusFailed {
			summary = chk.Error
		}
		fmt.Fprintf(tw, "%s\t%s\t%dms\t%s\n",
			chk.Name,
			strings.ToUpper(string(chk.Status)),
			chk.DurationMS,
			summary,
		)
		if chk.Status == workerhealth.StatusFailed && chk.Remediation != "" {
			fmt.Fprintf(tw, "\t\t\tremediation: %s\n", chk.Remediation)
		}
	}
	_ = tw.Flush()

	if report.Status == workerhealth.StatusOK {
		fmt.Fprintf(out, "\npreflight: PASSED (%d check(s) in %dms)\n", len(report.Checks), report.DurationMS)
	} else {
		fmt.Fprintf(out, "\npreflight: FAILED (exit code %d, %dms)\n", report.ExitCode, report.DurationMS)
	}
}
