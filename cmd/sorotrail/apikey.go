package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"os"
	"strconv"
	"text/tabwriter"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/khaylebfortune/sorotrail/internal/apikey"
	"github.com/khaylebfortune/sorotrail/internal/config"
	"github.com/khaylebfortune/sorotrail/internal/store"
)

// runAPIKey implements `sorotrail apikey`: issue, list, and revoke the
// API keys used to authenticate write/streaming requests when
// API_KEY_AUTH_ENABLED=true. The CLI talks to the database directly, so
// it is the bootstrap path for the first key (the /apikeys endpoints
// require an existing key).
func runAPIKey(args []string) error {
	if len(args) == 0 {
		return errors.New("apikey requires a subcommand: create, list, revoke")
	}
	switch args[0] {
	case "create":
		return apikeyCreate(args[1:])
	case "list":
		return apikeyList(args[1:])
	case "revoke":
		return apikeyRevoke(args[1:])
	case "help", "-h", "--help":
		apikeyUsage()
		return nil
	default:
		return fmt.Errorf("unknown apikey subcommand %q (want create|list|revoke)", args[0])
	}
}

func apikeyUsage() {
	fmt.Fprint(os.Stderr, `usage: sorotrail apikey <command>

Manages the API keys that authenticate write/streaming requests when
API_KEY_AUTH_ENABLED=true (see README, "API key authentication").

commands:
  create    issue a new key and print it (shown exactly once)
  list      list keys — prefixes only; secrets are never stored
  revoke    revoke a key by id; revoked keys stop working immediately
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
	fs := flag.NewFlagSet("apikey create", flag.ContinueOnError)
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
	st, cleanup, err := openStore(ctx)
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

	fmt.Printf("created API key %d (%s)\n", created.ID, created.Name)
	fmt.Printf("prefix: %s\n", created.Prefix)
	fmt.Printf("key:    %s\n", key)
	fmt.Println("store this key now — it will not be shown again.")
	return nil
}

func apikeyList(args []string) error {
	fs := flag.NewFlagSet("apikey list", flag.ContinueOnError)
	if err := fs.Parse(args); err != nil {
		return err
	}
	if len(fs.Args()) > 0 {
		return fmt.Errorf("unexpected argument %q", fs.Args()[0])
	}

	ctx := context.Background()
	st, cleanup, err := openStore(ctx)
	if err != nil {
		return err
	}
	defer cleanup()

	keys, err := st.ListAPIKeys(ctx)
	if err != nil {
		return err
	}
	if len(keys) == 0 {
		fmt.Println("no API keys")
		return nil
	}

	w := tabwriter.NewWriter(os.Stdout, 0, 4, 2, ' ', 0)
	fmt.Fprintln(w, "ID\tNAME\tPREFIX\tCREATED\tSTATUS")
	for _, k := range keys {
		status := "active"
		if k.Revoked() {
			status = "revoked"
		}
		fmt.Fprintf(w, "%d\t%s\t%s\t%s\t%s\n",
			k.ID, k.Name, k.Prefix, k.CreatedAt.Format("2006-01-02 15:04:05"), status)
	}
	return w.Flush()
}

func apikeyRevoke(args []string) error {
	fs := flag.NewFlagSet("apikey revoke", flag.ContinueOnError)
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
	st, cleanup, err := openStore(ctx)
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
	fmt.Printf("revoked API key %d\n", id)
	return nil
}
