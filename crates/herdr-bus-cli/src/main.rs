//! The `herdr-bus` command, all of which is `herdr_bus_cli::run`.

fn main() -> std::process::ExitCode {
    herdr_bus_cli::run(std::env::args_os())
}
