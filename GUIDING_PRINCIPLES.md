# Guiding Principles

*[Return to Main README](README.md)*

These principles reflect the observable design choices of `openas` and guide future changes and reviews.

1. **Simple, familiar streaming API with low import cost.** Keep `openas()`, `OpenAsReader`, and `OpenAsWriter` close to standard Python file-object conventions, and lazily import all optional codecs and remote transport dependencies so importing the package stays fast.
2. **Focused stream scope over kitchen-sink dependencies.** Support local files, `http`/`https`, `ftp`, and `ssh`/`sftp` with stream codecs (`.gz`, `.bz2`, `.xz`, `.zst`). Explicitly reject cloud object stores (`s3://`, `gs://`, `az://`, `hdfs://`) and container archives (`.zip`, `.tar.*`, `.rar`, `.7z`) rather than pulling in cloud SDKs or risking archive corruption.
3. **Safe defaults that protect user data and credentials.** Move existing outputs to sequential `.bu` backups by default on write, validate options before touching disk, refuse to rename directories as backups, verify SSH host keys against system known hosts, keep HTTP sessions cookie-free, and redact URL credentials and query tokens in logs and error messages.
4. **Deterministic, idempotent resource teardown.** Every acquired handle (file, codec wrapper, SFTP/SSH client, FTP host, HTTP response, progress bar) must close cleanly on normal exit, early iteration break, and exception paths, including interrupts.
5. **Honest boundaries and documentation.** Fail loudly with actionable errors when a capability is unsupported (such as HTTP writes, FTP append, container archives, or unknown URL schemes), and keep documentation synchronized with what the code actually does today.
6. **Verify observable behavior.** Test real outputs, side effects, handle lifecycles, and error contracts (including independent codec verification and realistic transport contracts) rather than pinning internal source layout.
