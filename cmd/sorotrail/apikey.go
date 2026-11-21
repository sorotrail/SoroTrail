package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"strconv"
	"text/tabwriter"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/sorotrail/sorotrail/internal/apikey"
	"github.com/sorotrail/sorotrail/internal/config"
	"github.com/sorotrail/sorotrail/internal/store"
)

// apiKeyStore abstracts the store calls required by the apikey subcommands.
type apiKeyStore interface {
	CreateAPIKey(ctx context.Context, k store.APIKey) (store.APIKey, error)
	GetAPIKey(ctx context.Context, id int64) (store.APIKey, error)
	ListAPIKeys(ctx context.Context) ([]store.APIKey, error)
	RevokeAPIKey(ctx context.Context, id int64) error
}

// openAPIKeyStore is the store opener seam used by apikey subcommands.
var openAPIKeyStore = func(ctx context.Context) (apiKeyStore, func(), error) {
	return openStore(ctx)
}

// runAPIKey implements `sorotrail apikey`: issue, list, and revoke the
// API keys used to authenticate write/streaming requests when
// API_KEY_AUTH_ENABLED=true. The CLI talks to the database directly, so
// it is the bootstrap path for the first key (the /apikeys endpoints
// require an existing key).
func runAPIKey(args []string) error {
	return runAPIKeyTo(os.Stdout, args)
}

// runAPIKeyTo is runAPIKey with an explicit output writer for testability.
func runAPIKeyTo(w io.Writer, args []string) error {
	if len(args) == 0 {
		return errors.New("apikey requires a subcommand: create, list, revoke")
	}
	switch args[0] {
	case "create":
		return apikeyCreateTo(w, args[1:])
	case "list":
		return apikeyListTo(w, args[1:])
	case "revoke":
		return apikeyRevokeTo(w, args[1:])
	case "rotate":
		return apikeyRotateTo(w, args[1:])
	case "abandon-rotation":
		return apikeyAbandonRotationTo(w, args[1:])
	case "help", "-h", "--help":
		apikeyUsageTo(w)
		return nil
	default:
		return fmt.Errorf("unknown apikey subcommand %q (want create|list|revoke)", args[0])
	}
}

func apikeyUsage() {
	apikeyUsageTo(os.Stderr)
}

func apikeyUsageTo(w io.Writer) {
	fmt.Fprint(w, `usage: sorotrail apikey <command>

Manages the API keys that authenticate write/streaming requests when
API_KEY_AUTH_ENABLED=true (see README, "API key authentication").

commands:
  create             issue a new key and print it (shown exactly once)
  list               list keys — prefixes only; secrets are never stored
  revoke             revoke a key by id; revoked keys stop working immediately
  rotate             issue a successor key with overlapping validity while predecessor remains valid
  abandon-rotation   revoke the successor key of an ongoing rotation and keep predecessor valid
`)
}

// openStore connects to the database (running migrations) and returns a
// ready store plus a cleanup func that closes its connection pool.
// Shared by every apikey subcommand.
func openStore(ctx context.Context) (*store.Postgres, func(), error) {
	cfg, err := config.Load()
	if err != nil {
		return nil, nil, err
	}
	if err := store.Migrate(cfg.DatabaseURL); err != nil {
		return nil, nil, err
	}
	pool, err := pgxpool.New(ctx, cfg.DatabaseURL)
	if err != nil {
		return nil, nil, fmt.Errorf("connecting to postgres: %w", err)
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, nil, fmt.Errorf("pinging postgres: %w", err)
	}
	return store.NewPostgres(pool, int64(cfg.PartitionLedgerSpan)), pool.Close, nil
}

func apikeyCreate(args []string) error {
	return apikeyCreateTo(os.Stdout, args)
}

func apikeyCreateTo(w io.Writer, args []string) error {
	fs := flag.NewFlagSet("apikey create", flag.ContinueOnError)
	fs.SetOutput(w)
	fs.Usage = func() {
		fmt.Fprint(fs.Output(), `usage: sorotrail apikey create [--name NAME]

Issues a new API key. The full key is printed exactly once — only its
bcrypt hash is stored, so it cannot be recovered later. Save it now.

flags:
`)
		fs.PrintDefaults()
	}
	name := fs.String("name", "", "human-readable label for the key's owner/purpose")
	if err := fs.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return nil
		}
		return err
	}
	if len(fs.Args()) > 0 {
		return fmt.Errorf("unexpected argument %q (flags only, e.g. --name ops)", fs.Args()[0])
	}
	if len(*name) > 100 {
		return errors.New("--name must be at most 100 characters")
	}

	ctx := context.Background()
	st, cleanup, err := openAPIKeyStore(ctx)
	if err != nil {
		return err
	}
	defer cleanup()

	key, prefix, secret, err := apikey.Generate()
	if err != nil {
		return fmt.Errorf("generating API key: %w", err)
	}
	hash, err := apikey.HashSecret(secret)
	if err != nil {
		return fmt.Errorf("hashing API key secret: %w", err)
	}
	created, err := st.CreateAPIKey(ctx, store.APIKey{Name: *name, Prefix: prefix, KeyHash: hash})
	if err != nil {
		return err
	}

	fmt.Fprintf(w, "created API key %d (%s)\n", created.ID, created.Name)
	fmt.Fprintf(w, "prefix: %s\n", created.Prefix)
	fmt.Fprintf(w, "key:    %s\n", key)
	fmt.Fprintln(w, "store this key now — it will not be shown again.")
	return nil
}

func apikeyList(args []string) error {
	return apikeyListTo(os.Stdout, args)
}

func apikeyListTo(w io.Writer, args []string) error {
	fs := flag.NewFlagSet("apikey list", flag.ContinueOnError)
	fs.SetOutput(w)
	if err := fs.Parse(args); err != nil {
		return err
	}
	if len(fs.Args()) > 0 {
		return fmt.Errorf("unexpected argument %q", fs.Args()[0])
	}

	ctx := context.Background()
	st, cleanup, err := openAPIKeyStore(ctx)
	if err != nil {
		return err
	}
	defer cleanup()

	keys, err := st.ListAPIKeys(ctx)
	if err != nil {
		return err
	}
	if len(keys) == 0 {
		fmt.Fprintln(w, "no API keys")
		return nil
	}

	tw := tabwriter.NewWriter(w, 0, 4, 2, ' ', 0)
	fmt.Fprintln(tw, "ID\tNAME\tPREFIX\tCREATED\tSTATUS")
	for _, k := range keys {
		status := "active"
		if k.Revoked() {
			status = "revoked"
		}
		fmt.Fprintf(tw, "%d\t%s\t%s\t%s\t%s\n",
			k.ID, k.Name, k.Prefix, k.CreatedAt.Format("2006-01-02 15:04:05"), status)
	}
	return tw.Flush()
}

func apikeyRevoke(args []string) error {
	return apikeyRevokeTo(os.Stdout, args)
}

func apikeyRevokeTo(w io.Writer, args []string) error {
	fs := flag.NewFlagSet("apikey revoke", flag.ContinueOnError)
	fs.SetOutput(w)
	fs.Usage = func() {
		fmt.Fprint(fs.Output(), `usage: sorotrail apikey revoke ID

Revokes the API key with the given ID. Revoked keys stop working on the
next request; the action cannot be undone (issue a new key instead).
`)
	}
	if err := fs.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return nil
		}
		return err
	}
	if len(fs.Args()) != 1 {
		fs.Usage()
		return errors.New("exactly one key ID is required")
	}
	id, err := strconv.ParseInt(fs.Args()[0], 10, 64)
	if err != nil || id <= 0 {
		return fmt.Errorf("invalid API key id %q", fs.Args()[0])
	}

	ctx := context.Background()
	st, cleanup, err := openAPIKeyStore(ctx)
	if err != nil {
		return err
	}
	defer cleanup()

	if err := st.RevokeAPIKey(ctx, id); err != nil {
		if errors.Is(err, store.ErrNotFound) {
			return fmt.Errorf("API key %d not found", id)
		}
		return err
	}
	fmt.Fprintf(w, "revoked API key %d\n", id)
	return nil
}

func apikeyRotate(args []string) error {
	return apikeyRotateTo(os.Stdout, args)
}

func apikeyRotateTo(w io.Writer, args []string) error {
	fs := flag.NewFlagSet("apikey rotate", flag.ContinueOnError)
	fs.SetOutput(w)
	fs.Usage = func() {
		fmt.Fprint(fs.Output(), `usage: sorotrail apikey rotate ID [--expires-in DURATION]

Issues a successor key for an existing predecessor key, setting an expiry
on the predecessor so zero-downtime rotation can proceed safely.

flags:
`)
		fs.PrintDefaults()
	}
	expiresIn := fs.Duration("expires-in", 24*time.Hour, "duration until predecessor key expires during overlap")
	if err := fs.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return nil
		}
		return err
	}
	if len(fs.Args()) != 1 {
		fs.Usage()
		return errors.New("exactly one predecessor key ID is required")
	}
	predecessorID, err := strconv.ParseInt(fs.Args()[0], 10, 64)
	if err != nil || predecessorID <= 0 {
		return fmt.Errorf("invalid predecessor API key id %q", fs.Args()[0])
	}

	ctx := context.Background()
	st, cleanup, err := openAPIKeyStore(ctx)
	if err != nil {
		return err
	}
	defer cleanup()

	pred, err := st.GetAPIKey(ctx, predecessorID)
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			return fmt.Errorf("predecessor API key %d not found", predecessorID)
		}
		return err
	}
	if pred.Revoked() {
		return fmt.Errorf("cannot rotate revoked predecessor API key %d", predecessorID)
	}

	key, prefix, secret, err := apikey.Generate()
	if err != nil {
		return fmt.Errorf("generating successor API key: %w", err)
	}
	hash, err := apikey.HashSecret(secret)
	if err != nil {
		return fmt.Errorf("hashing successor API key secret: %w", err)
	}

	expiresAt := time.Now().Add(*expiresIn)
	created, err := st.CreateAPIKey(ctx, store.APIKey{
		Name:          pred.Name + "-successor",
		Prefix:        prefix,
		KeyHash:       hash,
		PredecessorID: &predecessorID,
		ExpiresAt:     &expiresAt,
	})
	if err != nil {
		return err
	}

	fmt.Fprintf(w, "rotated API key %d -> successor %d (%s)\n", predecessorID, created.ID, created.Name)
	fmt.Fprintf(w, "prefix:        %s\n", created.Prefix)
	fmt.Fprintf(w, "key:           %s\n", key)
	fmt.Fprintf(w, "expires-at:    %s\n", expiresAt.Format("2006-01-02 15:04:05"))
	fmt.Fprintln(w, "store this key now — it will not be shown again.")
	return nil
}

func apikeyAbandonRotation(args []string) error {
	return apikeyAbandonRotationTo(os.Stdout, args)
}

func apikeyAbandonRotationTo(w io.Writer, args []string) error {
	fs := flag.NewFlagSet("apikey abandon-rotation", flag.ContinueOnError)
	fs.SetOutput(w)
	fs.Usage = func() {
		fmt.Fprint(fs.Output(), `usage: sorotrail apikey abandon-rotation SUCCESSOR_ID

Abandons an ongoing rotation by revoking the successor key and leaving
the predecessor key valid.
`)
	}
	if err := fs.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return nil
		}
		return err
	}
	if len(fs.Args()) != 1 {
		fs.Usage()
		return errors.New("exactly one successor key ID is required")
	}
	successorID, err := strconv.ParseInt(fs.Args()[0], 10, 64)
	if err != nil || successorID <= 0 {
		return fmt.Errorf("invalid successor API key id %q", fs.Args()[0])
	}

	ctx := context.Background()
	st, cleanup, err := openAPIKeyStore(ctx)
	if err != nil {
		return err
	}
	defer cleanup()

	succ, err := st.GetAPIKey(ctx, successorID)
	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			return fmt.Errorf("successor API key %d not found", successorID)
		}
		return err
	}
	if succ.PredecessorID == nil {
		return fmt.Errorf("API key %d is not a successor key (no predecessor recorded)", successorID)
	}

	if err := st.RevokeAPIKey(ctx, successorID); err != nil {
		return err
	}

	fmt.Fprintf(w, "abandoned rotation: revoked successor API key %d (predecessor %d remains valid)\n", successorID, *succ.PredecessorID)
	return nil
}
