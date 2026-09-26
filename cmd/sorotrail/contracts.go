package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strings"
	"text/tabwriter"
	"time"

	"github.com/sorotrail/sorotrail/internal/config"
	"github.com/sorotrail/sorotrail/pkg/client"
)

const contractsDefaultTimeout = 30 * time.Second

// runContracts implements `sorotrail contracts`: manage the operator's
// persisted watched-contract set through the same HTTP API used by other
// remote operators. The API key is sent as X-API-Key because the management
// endpoint deliberately does not accept a tenant Bearer credential.
func runContracts(args []string) error {
	return runContractsTo(os.Stdout, os.Stderr, args)
}

func runContractsTo(stdout, stderr io.Writer, args []string) error {
	if len(args) == 0 {
		contractsUsage(stderr)
		return errors.New("contracts requires a subcommand: add, list, or remove")
	}

	switch args[0] {
	case "add":
		return contractsAdd(stdout, stderr, args[1:])
	case "list":
		return contractsList(stdout, stderr, args[1:])
	case "remove":
		return contractsRemove(stdout, stderr, args[1:])
	case "help", "-h", "--help":
		contractsUsage(stderr)
		return nil
	default:
		contractsUsage(stderr)
		return fmt.Errorf("unknown contracts subcommand %q (want add|list|remove)", args[0])
	}
}

func contractsUsage(w io.Writer) {
	fmt.Fprint(w, `usage: sorotrail contracts <add|list|remove> [flags]

Manages the persisted watched-contract set through the SoroTrail HTTP API.
The default API URL is http://$HTTP_ADDR (or http://127.0.0.1:8080), and
--api-key defaults to the API_KEY environment variable.

commands:
  add      add a contract ID to the watch set
  list     list contract IDs and when they were added
  remove   remove a contract ID from the watch set

Use --confirm when the operation changes between all-contract and explicit
watch-list ingestion. The server requires confirmation for that transition.

common flags:
  --url URL       API root URL (default: http://$HTTP_ADDR)
  --api-key KEY   management key sent in X-API-Key (default: $API_KEY)
  --timeout DUR   HTTP request timeout (default: 30s)
`)
}

type contractsOptions struct {
	baseURL string
	apiKey  string
	timeout time.Duration
}

func contractsAdd(stdout, stderr io.Writer, args []string) error {
	_, options, contractID, confirm, err := parseContractsMutationFlags("contracts add", stderr, args)
	if errors.Is(err, flag.ErrHelp) {
		return nil
	}
	if err != nil {
		return err
	}

	ctx := context.Background()
	c := newContractsClient(options)
	result, err := c.AddWatchedContract(ctx, client.AddWatchedContractParams{
		Confirm: confirm,
	}, client.WatchedContractRequest{ContractID: contractID})
	if err != nil {
		return fmt.Errorf("adding watched contract: %w", err)
	}

	fmt.Fprintf(stdout, "added watched contract %s\n", contractID)
	if result.ModeTransition != "" {
		fmt.Fprintf(stdout, "ingestion mode: %s\n", result.ModeTransition)
	}
	if result.HistoryFromLedger > 0 {
		fmt.Fprintf(stdout, "history starts at ledger %d\n", result.HistoryFromLedger)
	}
	return nil
}

func contractsList(stdout, stderr io.Writer, args []string) error {
	_, options, err := parseContractsListFlags("contracts list", stderr, args)
	if errors.Is(err, flag.ErrHelp) {
		return nil
	}
	if err != nil {
		return err
	}

	page, err := newContractsClient(options).ListWatchedContracts(context.Background())
	if err != nil {
		return fmt.Errorf("listing watched contracts: %w", err)
	}
	if len(page.Contracts) == 0 {
		fmt.Fprintln(stdout, "no watched contracts")
		return nil
	}

	w := tabwriter.NewWriter(stdout, 0, 4, 2, ' ', 0)
	fmt.Fprintln(w, "CONTRACT_ID\tADDED_AT")
	for _, contract := range page.Contracts {
		fmt.Fprintf(w, "%s\t%s\n", contract.ContractID, contract.AddedAt)
	}
	return w.Flush()
}

func contractsRemove(stdout, stderr io.Writer, args []string) error {
	_, options, contractID, confirm, err := parseContractsMutationFlags("contracts remove", stderr, args)
	if errors.Is(err, flag.ErrHelp) {
		return nil
	}
	if err != nil {
		return err
	}

	result, err := newContractsClient(options).RemoveWatchedContract(
		context.Background(), contractID, client.RemoveWatchedContractParams{Confirm: confirm},
	)
	if err != nil {
		return fmt.Errorf("removing watched contract: %w", err)
	}

	fmt.Fprintf(stdout, "removed watched contract %s\n", contractID)
	if result.ModeTransition != "" {
		fmt.Fprintf(stdout, "ingestion mode: %s\n", result.ModeTransition)
	}
	fmt.Fprintln(stdout, "stored events were preserved")
	return nil
}

// parseContractsMutationFlags parses add/remove arguments. The contract ID
// may be positional (the documented form) or supplied with --contract for
// scripts that prefer a named flag; supplying both is rejected rather than
// silently choosing one.
func parseContractsMutationFlags(name string, stderr io.Writer, args []string) (*flag.FlagSet, contractsOptions, string, string, error) {
	fs := flag.NewFlagSet(name, flag.ContinueOnError)
	fs.SetOutput(stderr)
	fs.Usage = func() {
		fmt.Fprintf(fs.Output(), "usage: sorotrail %s [flags] CONTRACT_ID\n\n", name)
		fmt.Fprint(fs.Output(), "The contract ID must be a valid C-prefixed Stellar strkey.\n\nflags:\n")
		fs.PrintDefaults()
	}

	urlFlag := fs.String("url", "", "API root URL (defaults to http://$HTTP_ADDR)")
	apiKeyFlag := fs.String("api-key", "", "management key sent in X-API-Key (defaults to $API_KEY)")
	timeoutFlag := fs.Duration("timeout", contractsDefaultTimeout, "HTTP request timeout")
	contractFlag := fs.String("contract", "", "contract ID (alternative to the positional argument)")
	confirmFlag := fs.Bool("confirm", false, "confirm an all-contract/watch-list ingestion-mode transition")
	if err := fs.Parse(args); err != nil {
		return fs, contractsOptions{}, "", "", err
	}

	contractID, err := contractsContractID(fs.Args(), *contractFlag)
	if err != nil {
		return fs, contractsOptions{}, "", "", err
	}
	options, err := parseContractsOptions(*urlFlag, *apiKeyFlag, *timeoutFlag)
	if err != nil {
		return fs, contractsOptions{}, "", "", err
	}
	confirm := ""
	if *confirmFlag {
		confirm = "true"
	}
	return fs, options, contractID, confirm, nil
}

func parseContractsListFlags(name string, stderr io.Writer, args []string) (*flag.FlagSet, contractsOptions, error) {
	fs := flag.NewFlagSet(name, flag.ContinueOnError)
	fs.SetOutput(stderr)
	fs.Usage = func() {
		fmt.Fprintf(fs.Output(), "usage: sorotrail %s [flags]\n\nflags:\n", name)
		fs.PrintDefaults()
	}
	urlFlag := fs.String("url", "", "API root URL (defaults to http://$HTTP_ADDR)")
	apiKeyFlag := fs.String("api-key", "", "management key sent in X-API-Key (defaults to $API_KEY)")
	timeoutFlag := fs.Duration("timeout", contractsDefaultTimeout, "HTTP request timeout")
	if err := fs.Parse(args); err != nil {
		return fs, contractsOptions{}, err
	}
	if fs.NArg() != 0 {
		return fs, contractsOptions{}, fmt.Errorf("contracts list does not accept positional arguments")
	}
	options, err := parseContractsOptions(*urlFlag, *apiKeyFlag, *timeoutFlag)
	if err != nil {
		return fs, contractsOptions{}, err
	}
	return fs, options, nil
}

func contractsContractID(positional []string, flagValue string) (string, error) {
	if len(positional) > 1 {
		return "", errors.New("exactly one contract ID is required")
	}
	if len(positional) == 1 && flagValue != "" {
		return "", errors.New("contract ID supplied both positionally and with --contract")
	}
	id := flagValue
	if len(positional) == 1 {
		id = positional[0]
	}
	if !config.ValidContractID(id) {
		return "", errors.New("contract ID is required and must be a valid C-prefixed Stellar strkey")
	}
	return id, nil
}

func parseContractsOptions(rawURL, rawAPIKey string, timeout time.Duration) (contractsOptions, error) {
	if timeout <= 0 {
		return contractsOptions{}, errors.New("--timeout must be positive")
	}
	baseURL, err := contractsBaseURL(rawURL)
	if err != nil {
		return contractsOptions{}, err
	}
	apiKey := rawAPIKey
	if apiKey == "" {
		apiKey = os.Getenv("API_KEY")
	}
	return contractsOptions{baseURL: baseURL, apiKey: apiKey, timeout: timeout}, nil
}

func contractsBaseURL(raw string) (string, error) {
	if raw == "" {
		return "http://" + resolveHealthcheckAddr(""), nil
	}
	u, err := url.Parse(raw)
	if err != nil {
		return "", fmt.Errorf("--url is not a valid URL: %w", err)
	}
	if u.Scheme != "http" && u.Scheme != "https" {
		return "", errors.New("--url must use http:// or https://")
	}
	if u.Host == "" {
		return "", errors.New("--url must include a host")
	}
	if u.RawQuery != "" || u.Fragment != "" {
		return "", errors.New("--url must not include a query string or fragment")
	}
	u.Path = strings.TrimRight(u.Path, "/")
	return u.String(), nil
}

type contractsAPIKeyTransport struct {
	key  string
	base http.RoundTripper
}

func (t contractsAPIKeyTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	clone := req.Clone(req.Context())
	if t.key != "" {
		clone.Header.Set("X-API-Key", t.key)
	}
	base := t.base
	if base == nil {
		base = http.DefaultTransport
	}
	return base.RoundTrip(clone)
}

func newContractsClient(options contractsOptions) *client.Client {
	transport := &http.Client{
		Timeout: options.timeout,
		Transport: contractsAPIKeyTransport{
			key:  options.apiKey,
			base: http.DefaultTransport,
		},
	}
	return client.New(options.baseURL, client.WithHTTPClient(transport))
}
