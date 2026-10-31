package main

import (
	"bufio"
	"context"
	"database/sql"
	"encoding/json"
	"flag"
	"fmt"
	"net/http"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	pb "github.com/aldinokemal/go-whatsapp-web-multidevice/proto"
	mysql "github.com/go-sql-driver/mysql"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type candidate struct {
	ID                 string `json:"id"`
	Phone              string `json:"phone"`
	Usage              string `json:"usage,omitempty"`
	BroadcastDirection string `json:"broadcast_direction,omitempty"`
}

func isTwoWayBroadcast(c candidate) bool {
	direction := strings.ToLower(strings.TrimSpace(c.BroadcastDirection))
	if direction != "" && direction != "two_way" {
		return false
	}
	for _, usage := range strings.Split(c.Usage, ",") {
		if strings.EqualFold(strings.TrimSpace(usage), "broadcast") {
			return true
		}
	}
	return false
}

func orderRecoveryCandidates(candidates []candidate, previous map[string]string) {
	knownOnline := func(id string) bool { return previous[id] == "already_online" || previous[id] == "recovered_online" }
	sort.SliceStable(candidates, func(i, j int) bool {
		a, b := isTwoWayBroadcast(candidates[i]), isTwoWayBroadcast(candidates[j])
		if a != b {
			return a
		}
		return knownOnline(candidates[i].ID) && !knownOnline(candidates[j].ID)
	})
}

func fatal(err error) {
	if err != nil {
		panic(err)
	}
}
func normalize(j string) string {
	parts := strings.SplitN(j, "@", 2)
	if len(parts) != 2 {
		return j
	}
	return strings.SplitN(parts[0], ":", 2)[0] + "@" + parts[1]
}
func command(name string, args ...string) []byte {
	b, e := exec.Command(name, args...).Output()
	fatal(e)
	return b
}
func main() {
	pid := flag.String("pid", "", "bridge process, discovered from systemd by default")
	out := flag.String("out", "", "private operation directory")
	run := flag.Bool("run", false, "execute the approved recovery")
	concurrency := flag.Int("concurrency", 12, "bounded RPC concurrency")
	resume := flag.String("resume-results", "", "comma-separated previous result files, newest last")
	prioritize := flag.String("prioritize-results", "", "restore previously confirmed online accounts first after a bridge restart")
	rate := flag.Float64("rate", 3, "maximum Connect calls per second")
	adaptive := flag.Bool("adaptive", false, "adapt Connect pacing to queue age, capacity and drain progress")
	onlyTwoWay := flag.Bool("only-two-way-broadcast", false, "select only two-way broadcast accounts for an urgent priority pass")
	limit := flag.Int("limit", 0, "optional pilot size")
	flag.Parse()
	if *out == "" || *concurrency < 1 || *concurrency > 48 || *rate <= 0 || *rate > 8 {
		panic("invalid operation options")
	}
	fatal(os.MkdirAll(*out, 0700))
	if *pid == "" {
		*pid = strings.TrimSpace(strings.TrimPrefix(string(command("systemctl", "show", "go-whatsapp-ims-bridge-prod2.service", "-p", "MainPID")), "MainPID="))
	}
	if *pid == "" || *pid == "0" {
		panic("bridge is not running")
	}
	env, e := os.ReadFile("/proc/" + *pid + "/environ")
	fatal(e)
	values := map[string]string{}
	for _, item := range strings.Split(string(env), "\x00") {
		p := strings.SplitN(item, "=", 2)
		if len(p) == 2 {
			values[p[0]] = p[1]
		}
	}
	dsn := values["BRIDGE_ACCOUNT_DB_DSN"]
	if values["ACCOUNT_DB_DSN"] != "" {
		dsn = values["ACCOUNT_DB_DSN"]
	}
	if dsn == "" {
		panic("account database configuration missing")
	}
	cfg, e := mysql.ParseDSN(dsn)
	if e != nil {
		panic("invalid account database configuration")
	}
	cfg.Timeout = 5 * time.Second
	cfg.ReadTimeout = 15 * time.Second
	cfg.WriteTimeout = 10 * time.Second
	connector, e := mysql.NewConnector(cfg)
	if e != nil {
		panic("invalid database connector")
	}
	db := sql.OpenDB(connector)
	defer db.Close()
	db.SetMaxOpenConns(4)
	db.SetMaxIdleConns(4)
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	var serverID int64
	rows, e := db.QueryContext(ctx, "SELECT id FROM web_servers WHERE ip=? AND grpc_port=? AND deleted_at IS NULL", "192.151.156.2", 29092)
	fatal(e)
	servers := 0
	for rows.Next() {
		fatal(rows.Scan(&serverID))
		servers++
	}
	fatal(rows.Err())
	rows.Close()
	if servers != 1 {
		panic("API02 server assignment is not unique")
	}
	persisted := map[string]bool{}
	for _, j := range strings.Fields(string(command("docker", "exec", "gobridge-pg2", "psql", "-U", "whatsmeow", "-d", "whatsmeow", "-Atc", "SELECT jid FROM whatsmeow_device"))) {
		persisted[normalize(j)] = true
	}
	local := map[string]string{}
	data := command("sqlite3", "-separator", "\t", "/opt/go-whatsapp-ims-bridge-prod2/storages/chatstorage.db", "SELECT d.device_id,d.jid FROM devices d JOIN bridge_environments e ON e.account_id=d.device_id WHERE d.jid<>''")
	scanner := bufio.NewScanner(strings.NewReader(string(data)))
	for scanner.Scan() {
		p := strings.SplitN(scanner.Text(), "\t", 2)
		if len(p) == 2 {
			local[p[0]] = normalize(p[1])
		}
	}
	fatal(scanner.Err())
	rows, e = db.QueryContext(ctx, "SELECT CAST(a.id AS CHAR),a.phone,COALESCE(p.type,''),COALESCE(p.host,''),COALESCE(p.port,0),COALESCE(a.`usage`,''),COALESCE(a.broadcast_direction,'two_way')"+`
 FROM accounts a LEFT JOIN proxies p ON p.id=a.proxy_id
 WHERE a.deleted_at IS NULL AND a.web_server_id=? AND a.web_online IN (1,2)
 AND a.status NOT IN (1,2,5) ORDER BY a.web_online,a.id`, serverID)
	fatal(e)
	var candidates []candidate
	skipped := map[string]int{}
	total := 0
	for rows.Next() {
		var c candidate
		var kind, host string
		var port int
		fatal(rows.Scan(&c.ID, &c.Phone, &kind, &host, &port, &c.Usage, &c.BroadcastDirection))
		total++
		if *onlyTwoWay && !isTwoWayBroadcast(c) {
			skipped["other_purpose"]++
			continue
		}
		j, ok := local[c.ID]
		if !ok {
			skipped["missing_local_environment"]++
			continue
		}
		if !persisted[j] {
			skipped["missing_persisted_session"]++
			continue
		}
		if j != strings.TrimPrefix(c.Phone, "+")+"@s.whatsapp.net" {
			skipped["phone_jid_mismatch"]++
			continue
		}
		kind = strings.ToLower(strings.TrimSpace(kind))
		if (kind != "socks5" && kind != "http" && kind != "https") || strings.TrimSpace(host) == "" || port < 1 || port > 65535 {
			skipped["invalid_proxy"]++
			continue
		}
		candidates = append(candidates, c)
	}
	fatal(rows.Err())
	rows.Close()
	if *resume != "" {
		latest, err := readResults(*resume)
		fatal(err)
		filtered := candidates[:0]
		for _, c := range candidates {
			if resumeAccount(latest, c.ID) {
				filtered = append(filtered, c)
			} else {
				skipped["previous_terminal_result"]++
			}
		}
		candidates = filtered
	}
	previous := map[string]string{}
	if *prioritize != "" {
		latest, err := readResults(*prioritize)
		fatal(err)
		previous = latest
	}
	orderRecoveryCandidates(candidates, previous)
	priorityTotal := 0
	for _, c := range candidates {
		if isTwoWayBroadcast(c) {
			priorityTotal++
		}
	}
	plan := map[string]any{"server_id": serverID, "eligible_account_rows": total, "candidates": len(candidates), "two_way_broadcast_candidates": priorityTotal, "only_two_way_broadcast": *onlyTwoWay, "skipped": skipped, "concurrency": *concurrency, "max_connect_per_second": *rate, "adaptive": *adaptive, "at": time.Now().UTC()}
	b, _ := json.MarshalIndent(plan, "", "  ")
	fatal(os.WriteFile(filepath.Join(*out, "plan.json"), b, 0600))
	fmt.Println(string(b))
	b, _ = json.Marshal(candidates)
	fatal(os.WriteFile(filepath.Join(*out, "candidates.json"), b, 0600))
	if !*run {
		return
	}
	if *limit > 0 && len(candidates) > *limit {
		candidates = candidates[:*limit]
	}
	conn, e := grpc.NewClient("127.0.0.1:29092", grpc.WithTransportCredentials(insecure.NewCredentials()))
	fatal(e)
	defer conn.Close()
	client := pb.NewWhatsAppBridgeClient(conn)
	runCtx, stopRun := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stopRun()
	var pacingInterval atomic.Int64
	permits := recoveryPermits(runCtx, &pacingInterval)
	file, e := os.OpenFile(filepath.Join(*out, "results.jsonl"), os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0600)
	fatal(e)
	defer file.Close()
	var mu sync.Mutex
	counts := map[string]int{}
	var done atomic.Int64
	record := func(c candidate, result string) {
		mu.Lock()
		defer mu.Unlock()
		counts[result]++
		fatal(json.NewEncoder(file).Encode(map[string]any{"account_id": c.ID, "result": result, "at": time.Now().UTC()}))
		done.Add(1)
	}
	httpClient := &http.Client{Timeout: 4 * time.Second}
	health := &recoveryHealth{adaptive: *adaptive, maxRate: *rate}
	healthy := func() bool {
		ok := health.check(httpClient)
		if !ok {
			pacingInterval.Store(0)
			return false
		}
		allowed := *rate
		if *adaptive {
			allowed = health.rate
		}
		pacingInterval.Store(int64(float64(time.Second) / allowed))
		return true
	}
	status := func(id string) (*pb.AccountStatusResponse, error) {
		c, x := context.WithTimeout(runCtx, 8*time.Second)
		defer x()
		return client.GetAccountStatus(c, &pb.AccountStatusRequest{AccountId: id})
	}
	var ready atomic.Bool
	ready.Store(healthy())
	stop := make(chan struct{})
	var monitor sync.WaitGroup
	monitor.Add(1)
	progress := func() {
		mu.Lock()
		defer mu.Unlock()
		state := "running"
		if !ready.Load() {
			state = "waiting_for_health"
		}
		if runCtx.Err() != nil {
			state = "paused_with_checkpoint"
		} else if done.Load() == int64(len(candidates)) {
			state = "complete"
		}
		allowed := float64(0)
		if interval := pacingInterval.Load(); interval > 0 {
			allowed = float64(time.Second) / float64(interval)
		}
		b, _ := json.Marshal(map[string]any{"state": state, "total": len(candidates), "completed": done.Load(), "remaining": int64(len(candidates)) - done.Load(), "counts": counts, "ready": ready.Load(), "connect_per_second": allowed, "at": time.Now().UTC()})
		tmp, err := os.CreateTemp(*out, ".progress-*")
		fatal(err)
		_, err = tmp.Write(b)
		fatal(err)
		fatal(tmp.Close())
		fatal(os.Rename(tmp.Name(), filepath.Join(*out, "progress.json")))
		file.Sync()
		fmt.Println(string(b))
	}
	go func() {
		defer monitor.Done()
		t := time.NewTicker(5 * time.Second)
		defer t.Stop()
		n := 0
		for {
			select {
			case <-stop:
				return
			case <-t.C:
				ready.Store(healthy())
				n++
				if n%6 == 0 {
					progress()
				}
			}
		}
	}()
	jobs := make(chan candidate)
	var wg sync.WaitGroup
	for i := 0; i < *concurrency; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for c := range jobs {
				if err := waitReady(runCtx, &ready, 10*time.Minute); err != nil {
					stopRun()
					return
				}
				state, err := status(c.ID)
				if err != nil {
					record(c, "status_timeout_deferred")
					continue
				}
				if state.GetStatus() == "online" && state.GetIsUsable() {
					record(c, "already_online")
					continue
				}
				if strings.Contains(strings.ToLower(state.GetStatusDetail()), "explicitly disconnected") {
					record(c, "explicit_offline_skipped")
					continue
				}
				if state.GetStatus() == "logged_out" || state.GetStatus() == "qr_pending" {
					record(c, "login_required")
					continue
				}
				// Recheck assignment/status immediately before Connect, which itself fetches
				// the current database proxy instead of accepting any proxy from this helper.
				select {
				case <-runCtx.Done():
					return
				case <-permits:
				}
				if err := waitReady(runCtx, &ready, 10*time.Minute); err != nil {
					stopRun()
					return
				}
				check, x := context.WithTimeout(runCtx, 8*time.Second)
				var ok int
				err = db.QueryRowContext(check, "SELECT count(*) FROM accounts WHERE id=? AND deleted_at IS NULL AND web_server_id=? AND web_online IN (1,2) AND status NOT IN (1,2,5)", c.ID, serverID).Scan(&ok)
				x()
				if err != nil || ok != 1 {
					record(c, "eligibility_changed_skipped")
					continue
				}
				call, x := context.WithTimeout(runCtx, 55*time.Second)
				response, err := client.Connect(call, &pb.ConnectRequest{AccountId: c.ID})
				x()
				if err != nil {
					record(c, "connect_rpc_failed")
					continue
				}
				msg := strings.ToLower(response.GetMessage())
				if strings.Contains(msg, "connection not allowed by ruleset") {
					record(c, "proxy_blocked")
					continue
				}
				if strings.Contains(msg, "authentication failed") {
					record(c, "proxy_auth_failed")
					continue
				}
				if response.GetStatus() == "qr_pending" || response.GetStatus() == "logged_out" {
					record(c, "login_required")
					continue
				}
				result := "not_yet_online"
				for poll := 0; poll < 3; poll++ {
					time.Sleep(2 * time.Second)
					state, err = status(c.ID)
					if err != nil {
						result = "status_timeout_after_connect"
						break
					}
					if state.GetStatus() == "online" && state.GetIsUsable() {
						result = "recovered_online"
						break
					}
					if state.GetStatus() == "qr_pending" || state.GetStatus() == "logged_out" {
						result = "login_required"
						break
					}
				}
				record(c, result)
			}
		}()
	}
feed:
	for _, c := range candidates {
		select {
		case jobs <- c:
		case <-runCtx.Done():
			break feed
		}
	}
	close(jobs)
	wg.Wait()
	close(stop)
	monitor.Wait()
	progress()
	if runCtx.Err() != nil {
		fmt.Println("RECOVERY_PAUSED_WITH_CHECKPOINT")
	} else {
		fmt.Println("RECOVERY_PASS_COMPLETE")
	}
}
