# 上游同步与测试服部署 Runbook

本文记录当前 Go bridge 改造版同步原项目 `aldinokemal/go-whatsapp-web-multidevice` 的固定流程。

## 当前约定

- 本地仓库: `/Users/eric/Documents/GitHub/go-whatsapp-web-multidevice`
- Go module 目录: `/Users/eric/Documents/GitHub/go-whatsapp-web-multidevice/src`
- 当前 `origin`: `https://github.com/aldinokemal/go-whatsapp-web-multidevice.git`
- 测试服: `70.39.76.234`
- 测试服目录: `/opt/go-whatsapp-ims-bridge`
- 测试服源码: `/opt/go-whatsapp-ims-bridge/src`
- 测试服务: `go-whatsapp-ims-bridge.service`
- 测试 gRPC: `127.0.0.1:29091`
- 测试 health/metrics: `127.0.0.1:29191`
- 原 Node `ims-bridge` 保持共存，不在此流程中修改。

## 本地同步上游

在本地仓库根目录执行：

```bash
cd /Users/eric/Documents/GitHub/go-whatsapp-web-multidevice

git status --short
git fetch origin --tags --prune
git rev-list --left-right --count main...origin/main
git log --oneline --left-right --cherry-pick main...origin/main | sed -n '1,80p'

git merge --no-ff origin/main -m "同步上游 go-whatsapp-web-multidevice"
```

如果 `src/go.mod` / `src/go.sum` 冲突：

- 采用上游最新 `go.mau.fi/whatsmeow` 和 `golang.org/x/*` 版本。
- 保留 ims 兼容层新增依赖，例如 `github.com/nats-io/nats.go`、`google.golang.org/grpc`、`gopkg.in/yaml.v3`。
- 清理冲突标记后执行：

```bash
cd /Users/eric/Documents/GitHub/go-whatsapp-web-multidevice/src
GOPROXY=https://goproxy.cn,direct go mod tidy
```

如果官方 Go proxy 超时，也使用 `GOPROXY=https://goproxy.cn,direct`。

## 本地验证

```bash
cd /Users/eric/Documents/GitHub/go-whatsapp-web-multidevice/src

go test ./ui/bridge
go test ./...
go build -o /tmp/go-whatsapp-web-sync-test .
```

验证通过后完成 merge 提交：

```bash
cd /Users/eric/Documents/GitHub/go-whatsapp-web-multidevice
git add src/go.mod src/go.sum
git commit --no-edit
git status --short
```

## 部署测试服

同步源码：

```bash
rsync -az --delete --exclude '.git' \
  /Users/eric/Documents/GitHub/go-whatsapp-web-multidevice/src/ \
  root@70.39.76.234:/opt/go-whatsapp-ims-bridge/src/
```

远端测试和编译：

```bash
ssh root@70.39.76.234 '
  cd /opt/go-whatsapp-ims-bridge/src &&
  GOPROXY=https://goproxy.cn,direct go test ./ui/bridge &&
  GOPROXY=https://goproxy.cn,direct go test ./... &&
  GOPROXY=https://goproxy.cn,direct go build -o /opt/go-whatsapp-ims-bridge/go-whatsapp-web.new .
'
```

替换二进制并重启：

```bash
ssh root@70.39.76.234 '
  set -e
  cd /opt/go-whatsapp-ims-bridge
  backup="go-whatsapp-web.bak-$(date +%Y%m%d%H%M%S)"
  cp -a go-whatsapp-web "$backup"
  chmod +x go-whatsapp-web.new
  mv go-whatsapp-web.new go-whatsapp-web
  systemctl restart go-whatsapp-ims-bridge.service
  sleep 2
  systemctl --no-pager --full status go-whatsapp-ims-bridge.service | sed -n "1,22p"
  ss -ltnp | egrep ":(29091|29191)" || true
  curl -fsS http://127.0.0.1:29191/ready
'
```

## 部署后验证

```bash
ssh root@70.39.76.234 '
  cd /opt/go-whatsapp-ims-bridge/src &&
  grpcurl -plaintext -import-path proto -proto bridge.proto -d "{}" \
    127.0.0.1:29091 bridge.WhatsAppBridge/GetBridgeStats
'
```

检查账号状态：

```bash
ssh root@70.39.76.234 '
  cd /opt/go-whatsapp-ims-bridge/src &&
  for id in 1 2 3 4; do
    echo STATUS account=$id
    grpcurl -plaintext -import-path proto -proto bridge.proto \
      -d "{\"account_id\":\"$id\"}" \
      127.0.0.1:29091 bridge.WhatsAppBridge/GetAccountStatus
  done
'
```

如果重启后账号显示 offline，但后台 `web_online=2` 或账号需要继续测试，可手动恢复连接：

```bash
ssh root@70.39.76.234 '
  cd /opt/go-whatsapp-ims-bridge/src &&
  for row in "1 16723028367 2" "2 15812781311 1" "3 15812811131 1" "4 15812751827 1"; do
    set -- $row
    id=$1
    phone=$2
    tenant=$3
    echo CONNECT account=$id phone=$phone tenant=$tenant
    grpcurl -plaintext -import-path proto -proto bridge.proto \
      -d "{\"account_id\":\"$id\",\"phone_number\":\"$phone\",\"tenant_id\":\"$tenant\"}" \
      127.0.0.1:29091 bridge.WhatsAppBridge/Connect || true
  done
'
```

再检查后台在线状态：

```bash
ssh root@70.39.76.234 '
  cd /opt/ims-deploy &&
  docker exec ims-mysql sh -c '"'"'mysql -uroot -p$MYSQL_ROOT_PASSWORD imsdb -e "
    select id, tenant_id, phone, nickname, web_online, app_online, web_server_id, updated_at
    from accounts
    where deleted_at is null and id in (1,2,3,4)
    order by id;
  "'"'"'
'
```

`web_online=1` 表示后台已显示在线。

## 注意事项

- 每次同步后必须先跑本地测试，再部署测试服。
- 测试服部署前必须备份旧二进制。
- 不要改动原 Node `ims-bridge` 容器或服务。
- 如果没有配置自己的 fork，不要向当前 `origin` push。
- 上游频繁更新时，重点关注 `whatsmeow` API 变化是否影响：
  - `src/infrastructure/whatsapp/*`
  - `src/ui/bridge/*`
  - 消息回执、重连、代理、UA、动态和群功能兼容逻辑
