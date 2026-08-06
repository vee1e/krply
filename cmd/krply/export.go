package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/url"
	"os"
	"time"

	"github.com/spf13/cobra"

	queryv1 "github.com/krply/krply/api/query/v1"
	"github.com/krply/krply/internal/storage"
)

var (
	exportFormat   string
	exportSince    string
	exportUntil    string
	exportStreamID string
	exportOut      string
)

var exportCmd = &cobra.Command{
	Use:   "export",
	Short: "export journal records as newline-delimited JSON",
	Args:  cobra.NoArgs,
	RunE:  runExport,
}

func init() {
	rootCmd.AddCommand(exportCmd)
	f := exportCmd.Flags()
	f.StringVar(&exportFormat, "format", "ndjson", "output format (ndjson)")
	f.StringVar(&exportSince, "since", "", "export records observed after this time (RFC3339 or duration like 30m)")
	f.StringVar(&exportUntil, "until", "", "export records observed before this time (RFC3339 or duration like 30m)")
	f.StringVar(&exportStreamID, "stream-id", "", "restrict the export to one stream")
	f.StringVar(&exportOut, "out", "", "write to this file instead of stdout")
}

func runExport(cmd *cobra.Command, args []string) error {
	if exportFormat != "ndjson" {
		return fmt.Errorf("unsupported format %q (want ndjson)", exportFormat)
	}
	now := time.Now()
	since, err := parseTimeFlag(exportSince, now)
	if err != nil {
		return err
	}
	until, err := parseTimeFlag(exportUntil, now)
	if err != nil {
		return err
	}

	var w io.Writer = os.Stdout
	if exportOut != "" {
		fh, err := os.Create(exportOut)
		if err != nil {
			return err
		}
		defer fh.Close()
		w = fh
	}
	enc := json.NewEncoder(w)

	if serverURL != "" {
		return exportServer(cmd.Context(), enc, since, until)
	}

	store, err := openStore(storePath)
	if err != nil {
		return err
	}
	defer closeStore(store)

	recs, err := store.Events(cmd.Context(), storage.EventFilter{
		StreamID: exportStreamID,
		Since:    since,
		Until:    until,
	})
	if err != nil {
		return err
	}
	for i := range recs {
		if err := enc.Encode(&recs[i]); err != nil {
			return err
		}
	}
	return nil
}

func exportServer(ctx context.Context, enc *json.Encoder, since, until time.Time) error {
	c := newAPIClient(serverURL)
	cursor := ""
	for i := 0; i < 1000; i++ {
		q := url.Values{}
		if exportStreamID != "" {
			q.Set("stream_id", exportStreamID)
		}
		if !since.IsZero() {
			q.Set("since", since.Format(time.RFC3339))
		}
		if !until.IsZero() {
			q.Set("until", until.Format(time.RFC3339))
		}
		if cursor != "" {
			q.Set("cursor", cursor)
		}
		var page queryv1.EventPage
		if err := c.get(withQuery("/v1/events", q), &page); err != nil {
			return err
		}
		for i := range page.Items {
			if err := enc.Encode(&page.Items[i]); err != nil {
				return err
			}
		}
		if !page.HasMore || page.NextCursor == "" {
			return nil
		}
		cursor = page.NextCursor
	}
	return errors.New("export: too many pages")
}
