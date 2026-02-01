package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/exec"
	"strings"
	"sync"
	"time"
)

type AssignedOrder struct {
	OrderExists bool `json:"assigned"`
}

type Cluster struct {
	IP string
}

var (
	sshUser string

	proverFolders = map[int]string{
		1: "~/prover-1-aux-cluster",
		2: "~/prover-2-aux-cluster",
	}

	currentActiveProver = 0
	splitMode           = false
	mu                  sync.Mutex
	clusters            []Cluster
	prover1Endpoint     string
	prover2Endpoint     string
)

func mustLoadEnv() {
	ips := os.Getenv("CLUSTER_IPS")
	if ips == "" {
		log.Fatal("CLUSTER_IPS env var is required")
	}

	for _, ip := range strings.Split(ips, ",") {
		clusters = append(clusters, Cluster{IP: strings.TrimSpace(ip)})
	}

	prover1Endpoint = os.Getenv("PROVER1_ENDPOINT")
	prover2Endpoint = os.Getenv("PROVER2_ENDPOINT")

	if prover1Endpoint == "" || prover2Endpoint == "" {
		log.Fatal("PROVER1_ENDPOINT and PROVER2_ENDPOINT must be set")
	}

	sshUser = os.Getenv("SSH_USER")
	if sshUser == "" {
		sshUser = "user01"
	}
}

func sshDockerCompose(clusterIP, folder, action string) error {
	cmd := fmt.Sprintf("cd %s && docker compose %s", folder, action)

	sshCmd := exec.Command(
		"ssh",
		fmt.Sprintf("%s@%s", sshUser, clusterIP),
		cmd,
	)

	out, err := sshCmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("[%s] docker compose %s failed: %v\n%s",
			clusterIP, action, err, out)
	}

	log.Printf("[%s] docker compose %s (%s)", clusterIP, action, folder)
	return nil
}

func switchProver(target int) {
	mu.Lock()
	defer mu.Unlock()

	if target == currentActiveProver {
		return
	}

	log.Printf("Switching to prover %d", target)

	other := 1
	if target == 1 {
		other = 2
	}

	var wg sync.WaitGroup
	for _, c := range clusters {
		wg.Add(1)

		go func(cluster Cluster) {
			defer wg.Done()

			_ = sshDockerCompose(cluster.IP, proverFolders[other], "stop")
			_ = sshDockerCompose(cluster.IP, proverFolders[target], "start")
		}(c)
	}

	wg.Wait()
	currentActiveProver = target
	splitMode = false
	log.Printf("Prover %d active on all clusters", target)
}

func splitProvers() {
	mu.Lock()
	defer mu.Unlock()

	if splitMode {
		return
	}

	mid := len(clusters) / 2
	log.Printf("Splitting clusters: prover 1 gets %d, prover 2 gets %d", mid, len(clusters)-mid)

	var wg sync.WaitGroup
	for i, c := range clusters {
		wg.Add(1)

		go func(idx int, cluster Cluster) {
			defer wg.Done()

			if idx < mid {
				_ = sshDockerCompose(cluster.IP, proverFolders[2], "stop")
				_ = sshDockerCompose(cluster.IP, proverFolders[1], "start")
			} else {
				_ = sshDockerCompose(cluster.IP, proverFolders[1], "stop")
				_ = sshDockerCompose(cluster.IP, proverFolders[2], "start")
			}
		}(i, c)
	}

	wg.Wait()
	splitMode = true
	currentActiveProver = 0
	log.Printf("Split mode active: clusters 0-%d → prover 1, clusters %d-%d → prover 2",
		mid-1, mid, len(clusters)-1)
}

func checkOrder(url string) (bool, error) {
	resp, err := http.Get(url)
	if err != nil {
		return false, err
	}
	defer resp.Body.Close()

	var order AssignedOrder
	if err := json.NewDecoder(resp.Body).Decode(&order); err != nil {
		return false, err
	}

	return order.OrderExists, nil
}

func main() {
	mustLoadEnv()

	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	for range ticker.C {
		order1, err1 := checkOrder(prover1Endpoint)
		order2, err2 := checkOrder(prover2Endpoint)

		if err1 != nil || err2 != nil {
			log.Printf(
				"Endpoint error (err1=%v err2=%v) — defaulting to prover 1",
				err1, err2,
			)
			switchProver(1)
			continue
		}

		switch {
		case order1 && order2:
			splitProvers()
		case order1 && !order2:
			switchProver(1)
		case order2 && !order1:
			switchProver(2)
		default:
			log.Println("No orders — keeping current prover")
		}
	}
}
