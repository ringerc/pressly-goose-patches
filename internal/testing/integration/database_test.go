package integration

import (
	"testing"
	"time"

	"github.com/pressly/goose/v3"
	"github.com/pressly/goose/v3/database"
	"github.com/pressly/goose/v3/internal/testing/testdb"
	"github.com/stretchr/testify/require"
)

func TestPostgres(t *testing.T) {
	t.Parallel()

	db, cleanup, err := testdb.NewPostgres()
	require.NoError(t, err)
	t.Cleanup(cleanup)
	require.NoError(t, db.Ping())

	testDatabase(t, database.DialectPostgres, db, "testdata/migrations/postgres")
}

func TestSpanner(t *testing.T) {
	t.Parallel()

	db, cleanup, err := testdb.NewSpanner()
	require.NoError(t, err)
	t.Cleanup(cleanup)
	require.NoError(t, db.Ping())

	testDatabase(t, database.DialectSpanner, db, "testdata/migrations/spanner", goose.WithIsolateDDL(true))
}

func TestClickhouse(t *testing.T) {
	t.Parallel()

	db, cleanup, err := testdb.NewClickHouse()
	require.NoError(t, err)
	t.Cleanup(cleanup)
	require.NoError(t, db.Ping())

	testDatabase(t, database.DialectClickHouse, db, "testdata/migrations/clickhouse")

	type result struct {
		customerID     string    `db:"customer_id"`
		timestamp      time.Time `db:"time_stamp"`
		clickEventType string    `db:"click_event_type"`
		countryCode    string    `db:"country_code"`
		sourceID       int64     `db:"source_id"`
	}
	rows, err := db.Query(`SELECT * FROM clickstream ORDER BY customer_id`)
	require.NoError(t, err)
	var results []result
	for rows.Next() {
		var r result
		err = rows.Scan(&r.customerID, &r.timestamp, &r.clickEventType, &r.countryCode, &r.sourceID)
		require.NoError(t, err)
		results = append(results, r)
	}
	require.Equal(t, len(results), 3)
	require.NoError(t, rows.Close())
	require.NoError(t, rows.Err())

	parseTime := func(t *testing.T, s string) time.Time {
		t.Helper()
		tm, err := time.Parse("2006-01-02", s)
		require.NoError(t, err)
		return tm
	}
	want := []result{
		{"customer1", parseTime(t, "2021-10-02"), "add_to_cart", "US", 568239},
		{"customer2", parseTime(t, "2021-10-30"), "remove_from_cart", "", 0},
		{"customer3", parseTime(t, "2021-11-07"), "checkout", "", 307493},
	}
	for i, result := range results {
		require.Equal(t, result.customerID, want[i].customerID)
		require.Equal(t, result.timestamp, want[i].timestamp)
		require.Equal(t, result.clickEventType, want[i].clickEventType)
		if result.countryCode != "" && want[i].countryCode != "" {
			require.Equal(t, result.countryCode, want[i].countryCode)
		}
		require.Equal(t, result.sourceID, want[i].sourceID)
	}
}

func TestClickhouseReplicated(t *testing.T) {
	// Not t.Parallel(): the helper picks ephemeral host ports so multiple
	// runs *could* coexist, but the cluster startup is heavyweight (~10s),
	// so keeping this serial matches how the other integration tests behave.
	const cluster = "goose_cluster"
	t.Setenv(database.EnvClickhouseCluster, cluster)

	db, ch2, cleanup, err := testdb.NewClickHouseReplicated()
	require.NoError(t, err)
	t.Cleanup(cleanup)
	require.NoError(t, db.Ping())
	require.NoError(t, ch2.Ping())

	testDatabase(t, database.DialectClickHouseReplicated, db, "testdata/migrations/clickhouse-replicated", goose.WithIsolateDDL(true))

	// After testDatabase() completes, all up-migrations have been re-applied
	// (UpByOne loop at the end). Verify that the seeded rows and the
	// goose_db_version bookkeeping are visible on BOTH replicas. Replication
	// itself is a property of ClickHouse's Replicated* engines, not of the
	// dialect; what this checks is that the dialect drives them correctly
	// (ON CLUSTER DDL on both nodes, replicated version table, replicated
	// user tables via the migrations under testdata/) so the version state
	// actually converges on ch2.
	//
	// Replication is asynchronous by default; select_sequential_consistency
	// wouldn't help here because we're bypassing the dialect's Querier for a
	// raw SELECT.
	require.Eventually(t, func() bool {
		var got int
		if err := ch2.QueryRow(`SELECT count() FROM events`).Scan(&got); err != nil {
			return false
		}
		return got == 3
	}, 30*time.Second, 500*time.Millisecond, "expected 3 rows to replicate to ch2")

	require.Eventually(t, func() bool {
		var got int
		if err := ch2.QueryRow(`SELECT count() FROM (
			SELECT version_id, argMax(is_applied, tstamp) AS is_applied
			FROM goose_db_version GROUP BY version_id
		) WHERE version_id > 0 AND is_applied = 1`).Scan(&got); err != nil {
			return false
		}
		return got == 3
	}, 30*time.Second, 500*time.Millisecond, "expected 3 applied versions to replicate to ch2")
}

func TestClickhouseRemote(t *testing.T) {
	t.Parallel()

	db, cleanup, err := testdb.NewClickHouse()
	require.NoError(t, err)
	t.Cleanup(cleanup)
	require.NoError(t, db.Ping())
	testDatabase(t, database.DialectClickHouse, db, "testdata/migrations/clickhouse-remote")

	// assert that the taxi_zone_dictionary table has been created and populated
	var count int
	err = db.QueryRow(`SELECT count(*) FROM taxi_zone_dictionary`).Scan(&count)
	require.NoError(t, err)
	require.Equal(t, 265, count)
}

func TestMySQL(t *testing.T) {
	t.Parallel()

	db, cleanup, err := testdb.NewMariaDB()
	require.NoError(t, err)
	t.Cleanup(cleanup)
	require.NoError(t, db.Ping())

	testDatabase(t, database.DialectMySQL, db, "testdata/migrations/mysql")
}

func TestTurso(t *testing.T) {
	t.Parallel()

	db, cleanup, err := testdb.NewTurso()
	require.NoError(t, err)
	t.Cleanup(cleanup)
	require.NoError(t, db.Ping())

	testDatabase(t, database.DialectTurso, db, "testdata/migrations/turso")
}

func TestYDB(t *testing.T) {
	t.Parallel()

	db, cleanup, err := testdb.NewYdb()
	require.NoError(t, err)
	t.Cleanup(cleanup)
	require.NoError(t, db.Ping())

	testDatabase(t, database.DialectYdB, db, "testdata/migrations/ydb")
}

func TestStarrocks(t *testing.T) {
	t.Parallel()

	// t.Skip("Starrocks is flaky on CI, see https://github.com/pressly/goose/issues/881")

	db, cleanup, err := testdb.NewStarrocks()
	require.NoError(t, err)
	t.Cleanup(cleanup)
	require.NoError(t, db.Ping())

	testDatabase(t, database.DialectStarrocks, db, "testdata/migrations/starrocks", goose.WithIsolateDDL(true))
}
