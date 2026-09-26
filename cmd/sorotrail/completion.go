package main

import (
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"strings"
)

// completionCommand is one top-level subcommand offered by the shell
// completion scripts. name and desc must stay in sync with what
// dispatch() accepts and what usage() describes — the scripts are
// generated from this single slice so bash, zsh, and fish can never
// disagree with each other.
type completionCommand struct {
	name string
	desc string
}

// completionCommands mirrors the subcommand switch in dispatch().
// Keep the two in sync when adding a subcommand.
var completionCommands = []completionCommand{
	{"replay", "re-decode stored events with the current decoder"},
	{"apikey", "issue, list, and revoke API keys"},
	{"backfill", "ingest historical contract events from Horizon"},
	{"index-addresses", "rebuild the address to event inverted index"},
	{"migrate", "apply, roll back, or inspect database migrations"},
	{"healthcheck", "probe /health and exit (used by docker HEALTHCHECK)"},
	{"schema-inspect", "report migration state, partitions, and table sizes"},
	{"migrate-status", "report pending migrations without applying them"},
	{"completion", "print a shell completion script"},
	{"version", "print the build version, commit, and build date"},
	{"help", "show this help message"},
}

// completionNames returns just the subcommand names, in order.
func completionNames() []string {
	names := make([]string, 0, len(completionCommands))
	for _, c := range completionCommands {
		names = append(names, c.name)
	}
	return names
}

// runCompletion implements `sorotrail completion bash|zsh|fish`: it
// writes a completion script for the requested shell to stdout.
func runCompletion(args []string) error {
	return runCompletionTo(os.Stdout, args)
}

// runCompletionTo is runCompletion with an explicit output writer so
// tests can assert on the emitted script without touching stdout.
func runCompletionTo(w io.Writer, args []string) error {
	fs := flag.NewFlagSet("completion", flag.ContinueOnError)
	fs.Usage = func() {
		fmt.Fprint(fs.Output(), `usage: sorotrail completion <shell>

Prints a shell completion script for the given shell to stdout.
Completions cover the top-level subcommands; flags inside each
subcommand complete via that subcommand's own --help.

shells and install:
  bash   source <(sorotrail completion bash)
         (or drop it into /etc/bash_completion.d/sorotrail)
  zsh    sorotrail completion zsh > "${fpath[1]}/_sorotrail"
  fish   sorotrail completion fish > ~/.config/fish/completions/sorotrail.fish
`)
	}
	if err := fs.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return nil // usage already printed
		}
		return err
	}
	rest := fs.Args()
	if len(rest) != 1 {
		fs.Usage()
		return errors.New("completion requires exactly one shell argument: bash, zsh, or fish")
	}
	script, err := completionScript(rest[0])
	if err != nil {
		return err
	}
	_, err = fmt.Fprint(w, script)
	return err
}

// completionScript returns the completion script for shell, or an
// error naming the supported shells when shell is unknown.
func completionScript(shell string) (string, error) {
	switch shell {
	case "bash":
		return bashCompletionScript(), nil
	case "zsh":
		return zshCompletionScript(), nil
	case "fish":
		return fishCompletionScript(), nil
	default:
		return "", fmt.Errorf("unsupported shell %q (want bash, zsh, or fish)", shell)
	}
}

// bashCompletionScript renders a programmable-completion function for
// bash. First-word completion offers subcommands; later words fall
// back to flags and filenames (subcommand flags vary too much to
// enumerate statically here).
func bashCompletionScript() string {
	var b strings.Builder
	b.WriteString(`# bash completion for sorotrail
#
#   source <(sorotrail completion bash)
#
# or install it system-wide:
#
#   sorotrail completion bash > /etc/bash_completion.d/sorotrail

_sorotrail() {
    local cur
    cur="${COMP_WORDS[COMP_CWORD]}"
    if [ "$COMP_CWORD" -eq 1 ]; then
        COMPREPLY=($(compgen -W "`)
	b.WriteString(strings.Join(completionNames(), " "))
	b.WriteString(`" -- "$cur"))
    else
        COMPREPLY=($(compgen -W "--help -h" -- "$cur") $(compgen -f -- "$cur"))
    fi
}
complete -F _sorotrail sorotrail
`)
	return b.String()
}

// zshCompletionScript renders a #compdef script using _describe so
// each subcommand carries the same one-line description usage() shows.
func zshCompletionScript() string {
	var b strings.Builder
	b.WriteString(`#compdef sorotrail
#
# zsh completion for sorotrail — install with:
#
#   sorotrail completion zsh > "${fpath[1]}/_sorotrail"

_sorotrail() {
    if (( CURRENT == 2 )); then
        local -a cmds
        cmds=(
`)
	for _, c := range completionCommands {
		fmt.Fprintf(&b, "            '%s:%s'\n", c.name, c.desc)
	}
	b.WriteString(`        )
        _describe 'command' cmds
    else
        _files
    fi
}
compdef _sorotrail sorotrail
`)
	return b.String()
}

// fishCompletionScript renders one `complete` line per subcommand,
// guarded so subcommands only complete in the first argument position.
func fishCompletionScript() string {
	var b strings.Builder
	b.WriteString(`# fish completion for sorotrail
#
#   sorotrail completion fish > ~/.config/fish/completions/sorotrail.fish

complete -c sorotrail -f
`)
	for _, c := range completionCommands {
		fmt.Fprintf(&b,
			"complete -c sorotrail -n '__fish_use_subcommand' -a '%s' -d '%s'\n",
			c.name, c.desc)
	}
	return b.String()
}
