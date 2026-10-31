package whatsapp

import (
	"bytes"
	"context"
	"database/sql"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/aldinokemal/go-whatsapp-web-multidevice/pkg/sqlite"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"go.mau.fi/whatsmeow/store/sqlstore"
	"go.mau.fi/whatsmeow/types"
)

// The fixture is the unmodified v14 schema from whatsmeow 662ad1dc6900.
// Only disposable databases/schemas are used; no WhatsApp network calls occur.
func TestWhatsmeowUpgradeFromV14PreservesSessions(t *testing.T) {
	t.Run("sqlite", func(t *testing.T) {
		uri := sqlite.FormatChatStorageURI("file:"+filepath.Join(t.TempDir(), "sessions.db"), true, true)
		db, err := sql.Open(sqlite.DriverName, uri)
		require.NoError(t, err)
		db.SetMaxOpenConns(1)
		t.Cleanup(func() { _ = db.Close() })
		verifyWhatsmeowV14Upgrade(t, db, sqlite.DriverName)
	})
	t.Run("postgres", func(t *testing.T) {
		dsn := os.Getenv("BRIDGE_TEST_POSTGRES_DSN")
		if dsn == "" {
			t.Skip("set BRIDGE_TEST_POSTGRES_DSN to verify migration in an isolated PostgreSQL schema")
		}
		admin, err := sql.Open("postgres", dsn)
		require.NoError(t, err)
		admin.SetMaxOpenConns(1)
		// Exclude public from search_path so unqualified store queries cannot
		// read or change any real bridge session tables in this database.
		schema := "bridge_upgrade_test_" + strings.ReplaceAll(uuid.NewString(), "-", "")
		t.Cleanup(func() {
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			_, err := admin.ExecContext(ctx, "DROP SCHEMA IF EXISTS "+schema+" CASCADE")
			if err != nil {
				t.Errorf("remove isolated migration test schema: %v", err)
			}
			_ = admin.Close()
		})
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_, err = admin.ExecContext(ctx, "CREATE SCHEMA "+schema)
		require.NoError(t, err)
		// Put search_path in the connection parameters, not a one-off SET:
		// even if database/sql replaces a broken connection it stays isolated.
		scopedDSN := dsn + " search_path=" + schema
		if strings.HasPrefix(dsn, "postgres://") || strings.HasPrefix(dsn, "postgresql://") {
			parsed, err := url.Parse(dsn)
			require.NoError(t, err)
			query := parsed.Query()
			query.Set("search_path", schema)
			parsed.RawQuery = query.Encode()
			scopedDSN = parsed.String()
		}
		db, err := sql.Open("postgres", scopedDSN)
		require.NoError(t, err)
		db.SetMaxOpenConns(1)
		t.Cleanup(func() { _ = db.Close() })
		verifyWhatsmeowV14Upgrade(t, db, "postgres")
	})
}

func verifyWhatsmeowV14Upgrade(t *testing.T, db *sql.DB, dialect string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	schema, err := os.ReadFile("testdata/whatsmeow-v14.sql")
	require.NoError(t, err)
	_, err = db.ExecContext(ctx, string(schema))
	require.NoError(t, err)
	_, err = db.ExecContext(ctx, "CREATE TABLE whatsmeow_version (version INTEGER, compat INTEGER); INSERT INTO whatsmeow_version VALUES (14,8)")
	require.NoError(t, err)

	container := sqlstore.NewWithDB(db, dialect, nil)
	legacy := container.NewDevice()
	jid, err := types.ParseJID("15550001111:7@s.whatsapp.net")
	require.NoError(t, err)
	lid := types.NewJID("100000000000001", types.HiddenUserServer)
	peer := types.NewJID("15550002222", types.DefaultUserServer)
	_, err = db.ExecContext(ctx, `INSERT INTO whatsmeow_device (
		jid,lid,registration_id,noise_key,identity_key,signed_pre_key,signed_pre_key_id,signed_pre_key_sig,
		adv_key,adv_details,adv_account_sig,adv_account_sig_key,adv_device_sig,platform,push_name)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15)`,
		jid.String(), lid.String(), legacy.RegistrationID, legacy.NoiseKey.Priv[:], legacy.IdentityKey.Priv[:],
		legacy.SignedPreKey.Priv[:], legacy.SignedPreKey.KeyID, legacy.SignedPreKey.Signature[:],
		legacy.AdvSecretKey, []byte("fixture"), bytes.Repeat([]byte{1}, 64), bytes.Repeat([]byte{2}, 32), bytes.Repeat([]byte{3}, 64), "Chrome", "Legacy account")
	require.NoError(t, err)
	_, err = db.ExecContext(ctx, "INSERT INTO whatsmeow_lid_map(lid,pn) VALUES ($1,$2)", lid.User, jid.User)
	require.NoError(t, err)
	_, err = db.ExecContext(ctx, "INSERT INTO whatsmeow_sessions(our_jid,their_id,session) VALUES ($1,$2,$3)", jid.String(), peer.String(), []byte("preserve-signal-session"))
	require.NoError(t, err)
	_, err = db.ExecContext(ctx, "INSERT INTO whatsmeow_privacy_tokens(our_jid,their_jid,token,timestamp) VALUES ($1,$2,$3,$4)", jid.String(), peer.String(), []byte("preserve-privacy-token"), time.Now().Unix())
	require.NoError(t, err)
	_, err = db.ExecContext(ctx, "INSERT INTO whatsmeow_chat_settings(our_jid,chat_jid,pinned,archived) VALUES ($1,$2,true,true)", jid.String(), peer.String())
	require.NoError(t, err)

	require.NoError(t, container.Upgrade(ctx))
	require.NoError(t, container.Upgrade(ctx), "startup migration must be idempotent")
	var version, compat int
	require.NoError(t, db.QueryRowContext(ctx, "SELECT version,compat FROM whatsmeow_version").Scan(&version, &compat))
	require.Equal(t, 16, version)
	require.Equal(t, 8, compat, "new columns are declared backward compatible by upstream")
	loaded, err := container.GetDevice(ctx, jid)
	require.NoError(t, err)
	require.NotNil(t, loaded)
	require.Equal(t, jid, *loaded.ID)
	require.Equal(t, lid, loaded.LID)
	require.Equal(t, legacy.RegistrationID, loaded.RegistrationID)
	require.Equal(t, legacy.NoiseKey.Priv, loaded.NoiseKey.Priv)
	require.Equal(t, legacy.IdentityKey.Priv, loaded.IdentityKey.Priv)
	require.Equal(t, legacy.SignedPreKey.Priv, loaded.SignedPreKey.Priv)
	require.Equal(t, legacy.AdvSecretKey, loaded.AdvSecretKey)
	require.Equal(t, "Legacy account", loaded.PushName)
	require.Empty(t, loaded.CompanionMetaNonce)
	pn, err := container.LIDMap.GetPNForLID(ctx, lid)
	require.NoError(t, err)
	require.Equal(t, jid.ToNonAD(), pn)
	token, err := loaded.PrivacyTokens.GetPrivacyToken(ctx, peer)
	require.NoError(t, err)
	require.NotNil(t, token)
	require.Equal(t, []byte("preserve-privacy-token"), token.Token)
	var session []byte
	require.NoError(t, db.QueryRowContext(ctx, "SELECT session FROM whatsmeow_sessions WHERE our_jid=$1 AND their_id=$2", jid.String(), peer.String()).Scan(&session))
	require.Equal(t, []byte("preserve-signal-session"), session)
	var rootSecret string
	var pinned, archived bool
	require.NoError(t, db.QueryRowContext(ctx, "SELECT pinned,archived,wasa_root_secret_id FROM whatsmeow_chat_settings WHERE our_jid=$1 AND chat_jid=$2", jid.String(), peer.String()).Scan(&pinned, &archived, &rootSecret))
	require.True(t, pinned)
	require.True(t, archived)
	require.Empty(t, rootSecret)
	loaded.CompanionMetaNonce = "new-version-nonce"
	require.NoError(t, loaded.Save(ctx))
	reloaded, err := container.GetDevice(ctx, jid)
	require.NoError(t, err)
	require.Equal(t, "new-version-nonce", reloaded.CompanionMetaNonce)
}
