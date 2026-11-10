# Native deadline supervision

`buzz-supervise` watches trusted mission state and invokes a configured command
when an authoritative obligation is due. It is a host scheduler: it has no model
pool, agent identity, relay credentials or mission authority of its own. Normal
agent callbacks remain the fast path; a deadline can provide a fallback when a
callback is missing or a pool is asleep.

## Commands and platform requirements

Build with `cargo build --release -p buzz-acp --bin buzz-supervise`. Both entry
points use the same scheduler:

```sh
buzz-supervise --config /absolute/supervision.json
buzz-acp supervise --config /absolute/supervision.json
```

The ACP subcommand is selected before ordinary ACP credential parsing. On
Windows, launch **buzz-supervise.exe** directly from Task Scheduler or another
host process manager: its GUI subsystem creates no console. The ordinary ACP
binary retains its console subsystem. Helpers use `CREATE_NO_WINDOW` and a
suspended, kernel-managed Job Object with kill-on-close; job setup failure is
fatal. Use a startup/logon trigger and bounded restart-on-failure policy rather
than a repeating timer.

Linux requires systemd and a unified cgroup v2. Each supervisor must run directly
in its own dedicated `.service` cgroup whose name exactly matches
`linux_service_unit`. System and user service managers are supported. A shell
checkout, shared ACP service or ordinary standalone Linux invocation does not
provide the required containment and fails startup validation.

For example, install this as `mission-supervise.service` in the appropriate
system or user unit directory, then enable/start it through that manager:

```ini
[Unit]
Description=Mission deadline supervisor
StartLimitIntervalSec=300
StartLimitBurst=5

[Service]
Type=simple
ExecStart=/usr/local/bin/buzz-supervise --config /srv/mission/supervision.json
Restart=on-failure
RestartSec=15s
KillMode=control-group
TimeoutStopSec=15s
SendSIGKILL=yes

[Install]
WantedBy=default.target
```

Keep `KillMode=control-group`: helpers run in controlled process groups, but a
detached session can escape a group. The supervisor checks the bounded cgroup
tree after helper exit. A surviving descendant, timeout, output overflow or
containment failure causes a fatal exit so systemd reaps the whole unit before
restart. Commands that execute work remotely or inside an existing container
also need a deadline/cleanup mechanism there; a host cgroup cannot contain that
remote process.

## Configuration

The configuration file and all paths must be absolute. Commands are trusted
executable/argument vectors; there is no implicit shell interpolation. Install
the helpers and create the watch roots before starting the supervisor.
Configuration is read at startup; restart the managed instance to apply changes.

Watch roots are recursive and must be readable by the service user, including
their descendants. Use narrow authority roots and a dedicated configuration
directory; an unrelated private backup directory below a broad root prevents
watch registration. Isolate the active supervisor configuration rather than
changing permissions on unrelated backups. If retaining a legacy helper config
for rollback, document the active helper config path explicitly and preserve the
existing authority paths and shared helper serialization state.

```json
{
  "version": 1,
  "state_dir": "/srv/mission/supervisor-state",
  "watch_roots": ["/srv/mission/authority"],
  "watch_files": ["/srv/mission/authority/assignments.json"],
  "watch_subtrees": ["/srv/mission/authority/summaries", "/srv/mission/authority/receipts"],
  "projection": {
    "executable": "/usr/bin/python3",
    "args": ["/opt/mission/project.py", "/srv/mission/authority/assignments.json"],
    "timeout_secs": 60
  },
  "tick": {
    "executable": "/usr/bin/python3",
    "args": ["/opt/mission/check-due.py", "/srv/mission/authority/assignments.json"],
    "timeout_secs": 200
  },
  "helper_output_limit": 1048576,
  "linux_service_unit": "mission-supervise.service"
}
```

Use Windows absolute paths and `linux_service_unit: null` on Windows. Protect
the configuration and helpers against untrusted edits; invoking arbitrary
configured executables is an operator privilege. Keep signing credentials in
the existing trusted helper's private store rather than this file.

Limits: 32 existing watch roots, 4096 fixed files, 128 subtrees, 64 arguments per
command and 8192 bytes per argument. Projection timeout is 1–60 seconds; tick
timeout is 1–200 seconds. Captured stdout and stderr are each bounded by
`helper_output_limit`, between 1024 and 1048576 bytes (default 1048576). Configuration
and persisted JSON are bounded to 1 MiB. Watch inputs must stay within watch
roots and outside supervisor state.

`watch_files` selects exact authority files. `watch_subtrees` selects narrowly
defined lifecycle directories, including directories created later. Recursive
root subscriptions are established before the initial snapshot; path filters
exclude unrelated files. Ancestor create/remove/rename events trigger
reconciliation; parent-directory metadata changes alone do not. Keep transcripts,
logs, derived provenance and runtime freshness bindings outside selected
subtrees. Watcher overflow/error causes bounded resubscription, then a fatal
failure if resubscription keeps failing.

## Projection and due-command contract

The projection command writes one JSON document to stdout and exits zero:

```json
{
  "version": 1,
  "status": "armed",
  "generation": "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
  "next_due_at": 4102444800000,
  "obligation_key": "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
  "watch_inputs": ["/srv/mission/authority/assignments.json"],
  "gap_codes": [],
  "waiting_count": 0
}
```

`generation` and an armed `obligation_key` are 64 lowercase hexadecimal
characters. `next_due_at` is Unix **milliseconds**, not seconds. The logical key
must identify the same obligation across unrelated source generations.
`watch_inputs` adds at most 4096 absolute relevant file paths, including expected
missing files. Unknown extra fields are tolerated. Gap codes are bounded safe
identifiers: at most 128 values of 1–96 characters matching `[a-z0-9_-]`.
`waiting_count` cannot exceed 4096.

| Status | Meaning | Deadline |
| --- | --- | --- |
| `idle` | No obligations or unresolved diagnostics | Null; key null, gaps empty, waiting count zero |
| `armed` | At least one known authoritative candidate | Valid timestamp and logical key required |
| `waiting` | Work remains but the adapter grants no further action | Null |
| `gap` | Authority is unknown or unsafe to use | Null |

An armed candidate may carry partial diagnostics for other work. Unknown
authority never grants an alarm. The adapter owns inventory, terminal-state
reduction and finite wake/recovery allowances; the scheduler does not invent
work, extend those allowances, authorize delivery or mark a mission complete.

On startup or a relevant source change, the scheduler runs projection. Idle,
waiting and gap have no recurring helper timer. Armed state sleeps until the
nearest deadline. Before every due command, it reprojects; notifications during
a projection invalidate that result. The trusted due helper must also reread
its authority before publishing or acting.

Before invocation, the scheduler atomically persists the logical key and retry
deadline. A still-armed unresolved due key uses observation backoff from 10 to
300 seconds. The receipt survives restart; source generation changes alone do
not reset it. Fresh inactive state disarms that timer. Observation retries do
not reset or increase the adapter's signed-wake allowance.

A temporary helper failure must use **exit 75**. It becomes
`helper_temporary_failure`, fatal health and supervisor exit, allowing the host's
bounded failure restart policy to retry. For unknown authority, return valid
`gap` JSON with exit zero. Other nonzero projection exits become disarmed gaps;
other nonzero due exits trigger fresh projection and pending-only backoff.
Timeout, output and containment failures are always fatal.

## State, health and operational checks

One canonical `state_dir` identifies an instance. An OS exclusive lock prevents
duplicates and releases after a crash. Private `receipt.json` stores scheduling
counters/backoff, not mission authority. Atomic `health.json` exposes run ID,
supervisor PID, per-run projection/tick counts, phase, generation, logical key,
deadline and sanitized diagnostics. Health/state writes are not watch inputs.

After a terminal fixture has settled, an idle observation should retain the same
run ID/PID and unchanged projection/tick counts, with no helper descendants.
Exercise source write → projection → deadline → trusted action → terminal
reprojection separately; routing tests do not establish model reasoning or
semantic completion. A wake already published just before cancellation can
still be queued: the receiving agent must reconcile current authority before
substantive work. This scheduler provides bounded fallback and reconciliation,
not retraction of a previously published event.
