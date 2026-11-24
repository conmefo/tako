package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"sync/atomic"

	"github.com/conmefo/tako/internal/engine"
)

const usage = `usage:
  tako info [-n N] URL         probe URL and show the download plan
  tako get [-o FILE] URL       download URL
`

func main() {
	if len(os.Args) < 2 {
		fmt.Fprint(os.Stderr, usage)
		os.Exit(2)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()

	var err error
	switch os.Args[1] {
	case "info":
		err = cmdInfo(ctx, os.Args[2:])
	case "get":
		err = cmdGet(ctx, os.Args[2:])
	case "-h", "--help", "help":
		fmt.Print(usage)
		return
	default:
		fmt.Fprintf(os.Stderr, "unknown command %q\n%s", os.Args[1], usage)
		os.Exit(2)
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}
}

func cmdInfo(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("info", flag.ExitOnError)
	n := fs.Int("n", 4, "number of connections to plan for")
	fs.Parse(args)
	if fs.NArg() != 1 {
		return fmt.Errorf("info takes one URL")
	}

	info, err := engine.Probe(ctx, engine.NewClient(*n), fs.Arg(0))
	if err != nil {
		return err
	}

	fmt.Printf("URL:            %s\n", info.URL)
	if info.FinalURL != info.URL {
		fmt.Printf("Final URL:      %s\n", info.FinalURL)
	}
	if info.Size >= 0 {
		fmt.Printf("Size:           %s (%d bytes)\n", human(info.Size), info.Size)
	} else {
		fmt.Printf("Size:           unknown\n")
	}
	fmt.Printf("Ranges:         %s\n", rangesText(info))
	fmt.Printf("ETag:           %s\n", orNone(info.ETag))
	fmt.Printf("Last-Modified:  %s\n", orNone(info.LastModified))
	fmt.Printf("Filename:       %s\n", info.Filename)
	fmt.Printf("Plan:           %s\n", planText(info, engine.Plan(info, *n), *n))
	return nil
}

func cmdGet(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("get", flag.ExitOnError)
	out := fs.String("o", "", "output file (default: name from the server or URL)")
	fs.Parse(args)
	if fs.NArg() != 1 {
		return fmt.Errorf("get takes one URL")
	}

	client := engine.NewClient(1)
	info, err := engine.Probe(ctx, client, fs.Arg(0))
	if err != nil {
		return err
	}
	dst := *out
	if dst == "" {
		dst = info.Filename
	}
	fmt.Fprintf(os.Stderr, "%s → %s\n", info.FinalURL, dst)

	var done atomic.Int64
	stopProgress := startProgress(&done, info.Size)
	err = engine.Download(ctx, client, info.FinalURL, dst, func(n int) { done.Add(int64(n)) })
	stopProgress()
	return err
}

func rangesText(info engine.FileInfo) string {
	switch {
	case info.AcceptsRanges:
		return "yes (206 Partial Content)"
	case info.Status == 206:
		return "no (206 but total size unknown)"
	case info.Status == 200:
		return "no (server ignored the range, sent 200)"
	case info.Status == 416:
		return "no (416, empty file?)"
	}
	return "no"
}

func planText(info engine.FileInfo, ranges []engine.Range, asked int) string {
	switch {
	case len(ranges) == 0 && info.Size == 0:
		return "nothing to download (empty file)"
	case len(ranges) == 0:
		return "1 connection (size unknown, can't split)"
	case len(ranges) == 1 && asked > 1 && !info.AcceptsRanges:
		return "1 connection (no range support)"
	case len(ranges) == 1 && asked > 1:
		return fmt.Sprintf("1 connection (file smaller than %s × 2, not worth splitting)", human(engine.MinChunk))
	case len(ranges) == 1:
		return "1 connection"
	case len(ranges) < asked:
		return fmt.Sprintf("%d connections × %s (fewer than %d: pieces would be under %s)",
			len(ranges), human(ranges[0].Len()), asked, human(engine.MinChunk))
	}
	return fmt.Sprintf("%d connections × %s", len(ranges), human(ranges[0].Len()))
}

func orNone(s string) string {
	if s == "" {
		return "(none)"
	}
	return s
}
