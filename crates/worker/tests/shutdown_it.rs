//! DoD 3 / S1-P3-03: el apagado con SIGTERM debe drenar en menos de 6s.
//!
//! No requiere dependencias externas: con Redis inalcanzable el consumer
//! queda en el retry de conexion, que el token de apagado corta. El test
//! lanza el binario real, espera a que el HTTP acepte conexiones, envia
//! SIGTERM y mide hasta que el proceso termina.

use std::net::{TcpListener, TcpStream};
use std::process::{Child, Command, ExitStatus, Stdio};
use std::thread::sleep;
use std::time::{Duration, Instant};

const SHUTDOWN_BUDGET: Duration = Duration::from_secs(6);
const STARTUP_TIMEOUT: Duration = Duration::from_secs(10);
const EXIT_TIMEOUT: Duration = Duration::from_secs(10);

fn free_port() -> u16 {
    match TcpListener::bind("127.0.0.1:0") {
        Ok(listener) => match listener.local_addr() {
            Ok(addr) => addr.port(),
            Err(e) => panic!("no se pudo leer el puerto libre: {e}"),
        },
        Err(e) => panic!("no se pudo reservar un puerto libre: {e}"),
    }
}

fn wait_http(port: u16, limit: Duration) -> bool {
    let started = Instant::now();
    while started.elapsed() < limit {
        if TcpStream::connect_timeout(
            &format!("127.0.0.1:{port}")
                .parse()
                .unwrap_or_else(|e| panic!("direccion invalida: {e}")),
            Duration::from_millis(200),
        )
        .is_ok()
        {
            return true;
        }
        sleep(Duration::from_millis(50));
    }
    false
}

fn wait_exit(child: &mut Child, limit: Duration) -> Option<(Duration, ExitStatus)> {
    let started = Instant::now();
    loop {
        match child.try_wait() {
            Ok(Some(status)) => return Some((started.elapsed(), status)),
            Ok(None) => {}
            Err(e) => panic!("try_wait fallo: {e}"),
        }
        if started.elapsed() >= limit {
            return None;
        }
        sleep(Duration::from_millis(10));
    }
}

fn spawn_worker(port: u16) -> Child {
    Command::new(env!("CARGO_BIN_EXE_extraction-worker"))
        .env("HTTP_PORT", port.to_string())
        .env("RUST_LOG", "off")
        .env("LOG_FORMAT", "text")
        .env("REDIS_URL", "redis://127.0.0.1:1/")
        .env("S3_ENDPOINT", "http://127.0.0.1:9000")
        .env("S3_ACCESS_KEY", "worker-dev")
        .env("S3_SECRET_KEY", "worker-dev-secret")
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .unwrap_or_else(|e| panic!("no se pudo lanzar el binario: {e}"))
}

#[test]
fn sigterm_drena_en_menos_de_6s() {
    let port = free_port();
    let mut child = spawn_worker(port);

    if !wait_http(port, STARTUP_TIMEOUT) {
        let _ = child.kill();
        let _ = child.wait();
        panic!("el server HTTP no arranco en {STARTUP_TIMEOUT:?}");
    }

    // SAFETY: solo se envia una senal al PID del hijo recien creado.
    let rc = unsafe { libc::kill(child.id() as libc::pid_t, libc::SIGTERM) };
    assert_eq!(rc, 0, "kill(SIGTERM) fallo");

    let Some((elapsed, status)) = wait_exit(&mut child, EXIT_TIMEOUT) else {
        let _ = child.kill();
        let _ = child.wait();
        panic!("el worker no termino en {EXIT_TIMEOUT:?} tras SIGTERM");
    };
    assert!(
        status.success(),
        "el worker salio con error tras SIGTERM: {status:?}"
    );
    assert!(
        elapsed < SHUTDOWN_BUDGET,
        "el apagado tardo {elapsed:?}, debe ser < {SHUTDOWN_BUDGET:?}"
    );
}
