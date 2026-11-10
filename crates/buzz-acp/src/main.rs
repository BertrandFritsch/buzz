fn main() -> anyhow::Result<()> {
    if std::env::args_os()
        .nth(1)
        .is_some_and(|arg| arg == "supervise")
    {
        return buzz_acp::supervise::run(std::env::args_os().skip(2));
    }
    buzz_acp::run()
}
