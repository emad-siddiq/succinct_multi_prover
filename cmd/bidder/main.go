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
	IP       string
	Password string
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
	clusters        []Cluster
	apiEndpoint     string
	prover1Address  string
	prover2Address  string
)

func mustLoadEnv() {
	ips := os.Getenv("CLUSTER_IPS")
	if ips == "" {
		log.Fatal("CLUSTER_IPS env var is required")
	}

	ipList := strings.Split(ips, ",")

	passwords := os.Getenv("SSH_PASSWORDS")
	var passList []string
	if passwords != "" {
		passList = strings.Split(passwords, ",")
		if len(passList) != len(ipList) {
			log.Fatalf("SSH_PASSWORDS has %d entries but CLUSTER_IPS has %d — must match", len(passList), len(ipList))
		}
	}

	for i, ip := range ipList {
		c := Cluster{IP: strings.TrimSpace(ip)}
		if len(passList) > 0 {
			c.Password = strings.TrimSpace(passList[i])
		}
		clusters = append(clusters, c)
	}

	apiEndpoint = os.Getenv("API_ENDPOINT")
	prover1Address = os.Getenv("PROVER1_ADDRESS")
	prover2Address = os.Getenv("PROVER2_ADDRESS")

	if apiEndpoint == "" || prover1Address == "" || prover2Address == "" {
		log.Fatal("API_ENDPOINT, PROVER1_ADDRESS, and PROVER2_ADDRESS must be set")
	}

	sshUser = os.Getenv("SSH_USER")
	if sshUser == "" {
		sshUser = "user01"
	}
}

func sshDockerCompose(cluster Cluster, folder, action string) error {
	remoteCmd := fmt.Sprintf("cd %s && docker compose %s", folder, action)

	var sshCmd *exec.Cmd
	if cluster.Password != "" {
		sshCmd = exec.Command(
			"sshpass", "-p", cluster.Password,
			"ssh", "-o", "StrictHostKeyChecking=no",
			fmt.Sprintf("%s@%s", sshUser, cluster.IP),
			remoteCmd,
		)
	} else {
		sshCmd = exec.Command(
			"ssh",
			fmt.Sprintf("%s@%s", sshUser, cluster.IP),
			remoteCmd,
		)
	}

	out, err := sshCmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("[%s] docker compose %s failed: %v\n%s",
			cluster.IP, action, err, out)
	}

	log.Printf("[%s] docker compose %s (%s)", cluster.IP, action, folder)
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

			_ = sshDockerCompose(cluster, proverFolders[other], "stop")
			_ = sshDockerCompose(cluster, proverFolders[target], "start")
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
				_ = sshDockerCompose(cluster, proverFolders[2], "stop")
				_ = sshDockerCompose(cluster, proverFolders[1], "start")
			} else {
				_ = sshDockerCompose(cluster, proverFolders[1], "stop")
				_ = sshDockerCompose(cluster, proverFolders[2], "start")
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
		order1, err1 := checkOrder(apiEndpoint + "?prover=" + prover1Address)
		order2, err2 := checkOrder(apiEndpoint + "?prover=" + prover2Address)

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
