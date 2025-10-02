package main

import (
	"context"
	"encoding/json"
	"flag"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/VictoriaMetrics/sql-to-logsql/cmd/sql-to-logsql/api"
)

func main() {
	var cfg api.Config
	configFile := flag.String("config", "", "configuration file")
	flag.Parse()
	if *configFile != "" {
		configContent, err := os.ReadFile(*configFile)
		if err != nil {
			log.Fatalf("failed to read config file: %v", err)
		}
		if err = json.Unmarshal(configContent, &cfg); err != nil {
			log.Fatalf("failed to parse config file: %v", err)
		}
	}
	if cfg.ViewsDir == "" {
		cfg.ViewsDir = "./data/views"
	}
	if cfg.ListenAddr == "" {
		cfg.ListenAddr = ":8080"
	}
	if len(cfg.Tables) == 0 {
		cfg.Tables = map[string]string{"logs": "*"}
	}
	srv, err := api.NewServer(cfg)
	if err != nil {
		log.Fatalf("failed to configure server: %v", err)
	}

	httpServer := &http.Server{
		Addr:         cfg.ListenAddr,
		Handler:      srv,
		ReadTimeout:  60 * time.Second,
		WriteTimeout: 60 * time.Second,
		IdleTimeout:  60 * time.Second,
	}

	// Start server in background
	go func() {
		log.Printf("listening on %s", cfg.ListenAddr)
		if err := httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("server error: %v", err)
		}
	}()

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, os.Interrupt, syscall.SIGTERM)
	<-quit

	log.Println("shutting down server...")

	// Give server 30 seconds to finish requests
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	if err := httpServer.Shutdown(ctx); err != nil {
		log.Printf("server forced to shutdown: %v", err)
	}

	log.Println("server stopped")
}
