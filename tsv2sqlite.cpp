/**
 * tsv_to_sqlite.cpp
 * High-performance, secure TSV → SQLite importer with rich terminal UI.
 *
 * Build:
 *   g++ -O2 -std=c++20 tsv_to_sqlite.cpp -lsqlite3 -o tsv_to_sqlite
 */

#include <iostream>
#include <fstream>
#include <string>
#include <string_view>
#include <vector>
#include <filesystem>
#include <iomanip>
#include <charconv>
#include <system_error>
#include <chrono>
#include <csignal>
#include <cstdlib>
#include <cassert>
#include <sqlite3.h>

namespace fs = std::filesystem;

// ─────────────────────────── tunables ────────────────────────────────────────
constexpr long long  COMMIT_INTERVAL  = 500'000;       // rows per transaction
constexpr size_t     IO_BUFFER_SIZE   = 4 * 1024 * 1024; // 4 MiB read buffer
constexpr long long  MAX_BALANCE      = 2'100'000'000'000'000LL; // 21M BTC in sats
constexpr size_t     MAX_ADDRESS_LEN  = 128;            // reject oversized strings
// ─────────────────────────────────────────────────────────────────────────────

// ── RAII wrappers ─────────────────────────────────────────────────────────────

struct DB {
    sqlite3* handle = nullptr;
    ~DB() { if (handle) sqlite3_close(handle); }
    operator sqlite3*() const { return handle; }
};

struct Stmt {
    sqlite3_stmt* handle = nullptr;
    ~Stmt() { if (handle) sqlite3_finalize(handle); }
    operator sqlite3_stmt*() const { return handle; }
};

// ── Signal handling (clean shutdown) ─────────────────────────────────────────
static volatile std::sig_atomic_t g_interrupted = 0;
static void on_signal(int) { g_interrupted = 1; }

// ── Terminal UI helpers ───────────────────────────────────────────────────────
namespace ui {

// ANSI colour codes
constexpr const char* RESET  = "\033[0m";
constexpr const char* BOLD   = "\033[1m";
constexpr const char* DIM    = "\033[2m";
constexpr const char* GREEN  = "\033[38;2;80;220;120m";
constexpr const char* CYAN   = "\033[38;2;80;200;220m";
constexpr const char* YELLOW = "\033[38;2;220;200;60m";
constexpr const char* RED    = "\033[38;2;220;80;80m";
constexpr const char* GREY   = "\033[38;2;120;120;140m";
constexpr const char* WHITE  = "\033[38;2;230;230;235m";

static auto start_time = std::chrono::steady_clock::now();

void banner() {
    std::cout
        << "\n"
        << BOLD << CYAN
        << "  ┌─────────────────────────────────────────┐\n"
        << "  │    TSV → SQLite  High-Speed Importer    │\n"
        << "  └─────────────────────────────────────────┘"
        << RESET << "\n\n";
}

std::string human_size(uintmax_t bytes) {
    const char* units[] = {"B","KiB","MiB","GiB","TiB"};
    double v = static_cast<double>(bytes);
    int i = 0;
    while (v >= 1024.0 && i < 4) { v /= 1024.0; ++i; }
    std::ostringstream oss;
    if (i == 0) oss << static_cast<uintmax_t>(v) << " B";
    else        oss << std::fixed << std::setprecision(2) << v << " " << units[i];
    return oss.str();
}

void progress(long long rows, long long skipped, long long commits) {
    auto now  = std::chrono::steady_clock::now();
    double s  = std::chrono::duration<double>(now - start_time).count();
    double rps = (s > 0) ? rows / s : 0;

    // Build a compact progress bar (30 chars wide) based on millions of rows
    int filled = static_cast<int>(rows / 1'000'000) % 30;
    std::string bar;
    bar.reserve(30 * 3); // each UTF-8 block char is 3 bytes
    for (int i = 0; i < 30; ++i)
        bar += (i < filled) ? "\xe2\x96\x88" : "\xe2\x96\x91"; // █ or ░

    std::cout
        << "\r  "
        << CYAN  << bar << RESET
        << "  " << BOLD << WHITE  << std::setw(11) << rows   << RESET << " rows"
        << GREY  << " │ " << RESET
        << YELLOW << std::fixed << std::setprecision(0) << rps << RESET << " r/s"
        << GREY  << " │ skip:" << RESET
        << (skipped ? RED : GREY) << skipped << RESET
        << GREY  << " │ tx:" << RESET
        << DIM   << commits << RESET
        << "   " << std::flush;
}

void summary(long long rows, long long skipped, const std::string& dbFile) {
    auto now = std::chrono::steady_clock::now();
    double elapsed = std::chrono::duration<double>(now - start_time).count();

    std::error_code ec;
    auto sz = fs::file_size(dbFile, ec);
    std::string szStr = ec ? "?" : human_size(sz);

    std::cout
        << "\n\n"
        << BOLD << GREEN
        << "  ✔  Import complete\n" << RESET
        << GREY  << "  ─────────────────────────────────────────\n" << RESET
        << "  " << WHITE << "Rows inserted : " << RESET << BOLD << rows    << RESET << "\n"
        << "  " << WHITE << "Rows skipped  : " << RESET
             << (skipped ? RED : GREY)  << skipped   << RESET << "\n"
        << "  " << WHITE << "Elapsed       : " << RESET
             << std::fixed << std::setprecision(2) << elapsed << " s\n"
        << "  " << WHITE << "Throughput    : " << RESET
             << std::fixed << std::setprecision(0) << (elapsed > 0 ? rows/elapsed : 0)
             << " rows/s\n"
        << "  " << WHITE << "Database size : " << RESET << szStr << "\n"
        << GREY  << "  ─────────────────────────────────────────\n" << RESET
        << "\n";
}

void error(const std::string& msg) {
    std::cerr << "\n  " << RED << BOLD << "✘  Error: " << RESET << msg << "\n\n";
}

void warn(const std::string& msg) {
    std::cerr << "\r  " << YELLOW << "⚠  " << RESET << msg << "\n";
}

} // namespace ui

// ── SQLite helpers ────────────────────────────────────────────────────────────

static bool exec(sqlite3* db, const char* sql) {
    char* err = nullptr;
    int rc = sqlite3_exec(db, sql, nullptr, nullptr, &err);
    if (rc != SQLITE_OK) {
        ui::error(std::string("SQL: ") + (err ? err : "unknown"));
        sqlite3_free(err);
        return false;
    }
    return true;
}

// ── Input validation ──────────────────────────────────────────────────────────

/// Returns false if the TSV path looks dangerous (path traversal etc.)
static bool safe_path(const char* path) {
    std::string_view p(path);
    if (p.empty())          return false;
    if (p.size() > 4096)    return false;
    // Reject null bytes embedded in the string (shouldn't happen via argv, but be safe)
    if (p.find('\0') != std::string_view::npos) return false;
    return true;
}

// ─────────────────────────────────────────────────────────────────────────────
int main(int argc, char* argv[]) {
    std::signal(SIGINT,  on_signal);
    std::signal(SIGTERM, on_signal);

    ui::banner();

    if (argc != 3) {
        std::cerr << "  Usage: " << argv[0] << " <input.tsv> <output.db>\n\n";
        return EXIT_FAILURE;
    }

    // ── Validate paths ────────────────────────────────────────────────────────
    if (!safe_path(argv[1]) || !safe_path(argv[2])) {
        ui::error("Invalid file path supplied.");
        return EXIT_FAILURE;
    }

    const std::string inputFile = argv[1];
    const std::string dbFile    = argv[2];

    // Refuse to overwrite the input file with the database
    {
        std::error_code ec1, ec2;
        auto inAbs  = fs::canonical(inputFile, ec1);
        auto outAbs = fs::weakly_canonical(dbFile, ec2);
        if (!ec1 && !ec2 && inAbs == outAbs) {
            ui::error("Input and output paths resolve to the same file.");
            return EXIT_FAILURE;
        }
    }

    // ── Open input file ───────────────────────────────────────────────────────
    std::ifstream infile(inputFile, std::ios::binary);
    if (!infile.is_open()) {
        ui::error("Cannot open input file: " + inputFile);
        return EXIT_FAILURE;
    }
    std::vector<char> iobuf(IO_BUFFER_SIZE);
    infile.rdbuf()->pubsetbuf(iobuf.data(), static_cast<std::streamsize>(iobuf.size()));

    // ── Open / create SQLite database ─────────────────────────────────────────
    DB db;
    int rc = sqlite3_open(dbFile.c_str(), &db.handle);
    if (rc != SQLITE_OK) {
        ui::error("Cannot open database: " + std::string(sqlite3_errmsg(db)));
        return EXIT_FAILURE;
    }

    // Security: limit attack surface of SQLite
    sqlite3_limit(db, SQLITE_LIMIT_LENGTH,       MAX_ADDRESS_LEN * 4);
    sqlite3_limit(db, SQLITE_LIMIT_SQL_LENGTH,   4096);

    // Performance pragmas (trading durability for speed — acceptable for bulk import)
    if (!exec(db, "PRAGMA synchronous = OFF;"))           return EXIT_FAILURE;
    if (!exec(db, "PRAGMA journal_mode = MEMORY;"))       return EXIT_FAILURE;
    if (!exec(db, "PRAGMA cache_size = -262144;"))        return EXIT_FAILURE; // ~256 MiB
    if (!exec(db, "PRAGMA page_size = 4096;"))            return EXIT_FAILURE;
    if (!exec(db, "PRAGMA temp_store = MEMORY;"))         return EXIT_FAILURE;

    // ── Schema ────────────────────────────────────────────────────────────────
    const char* createSQL =
        "CREATE TABLE IF NOT EXISTS addresses ("
        "  address TEXT NOT NULL CHECK(length(address) BETWEEN 1 AND 128),"
        "  balance INTEGER NOT NULL CHECK(balance >= 0),"
        "  PRIMARY KEY (address)"
        ") WITHOUT ROWID;";

    if (!exec(db, createSQL)) return EXIT_FAILURE;

    // ── Prepared statement ────────────────────────────────────────────────────
    Stmt stmt;
    const char* insertSQL =
        "INSERT OR IGNORE INTO addresses (address, balance) VALUES (?, ?);";
    rc = sqlite3_prepare_v2(db, insertSQL, -1, &stmt.handle, nullptr);
    if (rc != SQLITE_OK) {
        ui::error(std::string("Prepare failed: ") + sqlite3_errmsg(db));
        return EXIT_FAILURE;
    }

    // ── Begin first transaction ───────────────────────────────────────────────
    if (!exec(db, "BEGIN TRANSACTION;")) return EXIT_FAILURE;

    // ── Skip header line ──────────────────────────────────────────────────────
    std::string line;
    line.reserve(256);
    if (!std::getline(infile, line)) {
        ui::error("Input file is empty.");
        return EXIT_FAILURE;
    }

    // ── Main import loop ──────────────────────────────────────────────────────
    long long rowCount     = 0;
    long long skipped      = 0;
    long long commitCount  = 0;
    long long txRows       = 0;

    while (!g_interrupted && std::getline(infile, line)) {
        // Strip trailing '\r' (Windows line endings)
        if (!line.empty() && line.back() == '\r')
            line.pop_back();

        if (line.empty()) continue;

        // ── Find tab delimiter ────────────────────────────────────────────────
        const size_t tabPos = line.find('\t');
        if (tabPos == std::string::npos || tabPos == 0) {
            if (skipped < 20) // throttle noise
                ui::warn("No tab on line " + std::to_string(rowCount + skipped + 1) + ", skipped.");
            ++skipped;
            continue;
        }

        // ── Validate address ──────────────────────────────────────────────────
        if (tabPos > MAX_ADDRESS_LEN) {
            ++skipped;
            continue;
        }

        // ── Parse balance with from_chars (no exceptions, no locale) ─────────
        long long balance = 0;
        const char* bStart = line.data() + tabPos + 1;
        const char* bEnd   = line.data() + line.size();

        // Allow optional leading '-' for negative detection, then reject below
        auto [ptr, ec2] = std::from_chars(bStart, bEnd, balance);
        if (ec2 != std::errc{} || ptr != bEnd) {
            if (skipped < 20)
                ui::warn("Bad balance near line " + std::to_string(rowCount + skipped + 1) + ", skipped.");
            ++skipped;
            continue;
        }
        if (balance < 0 || balance > MAX_BALANCE) {
            ++skipped;
            continue;
        }

        // ── Bind & execute ────────────────────────────────────────────────────
        // SQLITE_STATIC is safe: line is not modified until after sqlite3_reset
        sqlite3_bind_text(stmt, 1, line.data(), static_cast<int>(tabPos), SQLITE_STATIC);
        sqlite3_bind_int64(stmt, 2, balance);

        int stepRc = sqlite3_step(stmt);
        if (stepRc != SQLITE_DONE) {
            // SQLITE_CONSTRAINT_PRIMARYKEY is silent duplicate — the rest are real errors
            if (stepRc != SQLITE_CONSTRAINT) {
                ui::warn(std::string("Insert error: ") + sqlite3_errmsg(db));
            }
        }

        sqlite3_clear_bindings(stmt);
        sqlite3_reset(stmt);

        ++rowCount;
        ++txRows;

        // ── Periodic commit ───────────────────────────────────────────────────
        if (txRows >= COMMIT_INTERVAL) {
            if (!exec(db, "COMMIT;") || !exec(db, "BEGIN TRANSACTION;")) {
                ui::error("Transaction management failed.");
                return EXIT_FAILURE;
            }
            txRows = 0;
            ++commitCount;
            ui::progress(rowCount, skipped, commitCount);
        }
    }

    // ── Final commit ──────────────────────────────────────────────────────────
    if (!exec(db, "COMMIT;")) {
        ui::error("Final commit failed.");
        return EXIT_FAILURE;
    }
    ++commitCount;

    // ── Create index after bulk insert (much faster than during insert) ───────
    std::cout << "\n\n  " << ui::DIM << "Building index…" << ui::RESET << std::flush;
    if (!exec(db, "CREATE INDEX IF NOT EXISTS idx_balance ON addresses(balance DESC);")) {
        ui::warn("Index creation failed (non-fatal).");
    }

    // ── Summary ───────────────────────────────────────────────────────────────
    ui::summary(rowCount, skipped, dbFile);

    if (g_interrupted) {
        std::cout << "  " << ui::YELLOW << "Import was interrupted by user signal.\n" << ui::RESET;
    }

    return EXIT_SUCCESS;
}
