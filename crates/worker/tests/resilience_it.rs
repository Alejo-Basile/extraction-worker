//! Resiliencia bajo degradacion (S5-P3-02/S5-P3-03), sin dependencias
//! externas: breaker abierto no debe abrir conexiones y un S3 lento debe
//! cortarse por timeout sin bloquear el runtime.

use std::net::TcpListener;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use extraction_worker::consumer::pipeline::{ObjectSource, TempPdf};
use extraction_worker::consumer::resilience::{with_retry_cb, CircuitBreaker, Failure};
use extraction_worker::objects::{with_s3_timeout, ObjectStoreSource, S3Config};

fn must<T, E: std::fmt::Display>(result: Result<T, E>) -> T {
    match result {
        Ok(value) => value,
        Err(e) => panic!("operacion de test fallo: {e}"),
    }
}

fn is_s3_timeout(result: &Result<u64, Failure>) -> bool {
    matches!(
        result,
        Err(Failure::Transient { code, .. }) if code == "S3_TIMEOUT"
    )
}

struct SlowSource {
    calls: Arc<AtomicUsize>,
    delay: Duration,
}

impl ObjectSource for SlowSource {
    async fn download_to(
        &self,
        _key: &str,
        _max_bytes: u64,
        _dest: &std::path::Path,
    ) -> Result<u64, Failure> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        tokio::time::sleep(self.delay).await;
        Ok(0)
    }
}

#[tokio::test]
async fn breaker_abierto_no_abre_conexiones_de_red() {
    let listener = must(TcpListener::bind("127.0.0.1:0"));
    must(listener.set_nonblocking(true));
    let addr = must(listener.local_addr());
    let source = Arc::new(must(ObjectStoreSource::new(&S3Config {
        endpoint: format!("http://{addr}"),
        bucket_raw: "raw-pdfs".to_string(),
        bucket_txt: "extracted-txt".to_string(),
        access_key: "test".to_string(),
        secret_key: "test-secret".to_string(),
    })));
    let breaker = CircuitBreaker::new(1, Duration::from_secs(30));
    breaker.on_failure();
    assert!(breaker.is_open());

    let temp = must(TempPdf::create());
    let path = temp.path().to_path_buf();
    let key = "raw-pdfs/breaker.pdf".to_string();
    let result = with_retry_cb(&breaker, || {
        let source = source.clone();
        let path = path.clone();
        let key = key.clone();
        async move {
            with_s3_timeout(Duration::from_millis(100), || async {
                source.download_to(&key, 1024, &path).await
            })
            .await
        }
    })
    .await;

    match result {
        Err(Failure::CircuitOpen) => {}
        other => panic!("se esperaba CircuitOpen, llego {other:?}"),
    }

    tokio::time::sleep(Duration::from_millis(50)).await;
    match listener.accept() {
        Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => {}
        Ok(_) => panic!("el breaker abierto abrio una conexion de red"),
        Err(e) => panic!("accept inesperado: {e}"),
    }
}

#[tokio::test]
async fn s3_lento_dispara_timeout_acotado() {
    let calls = Arc::new(AtomicUsize::new(0));
    let source = SlowSource {
        calls: calls.clone(),
        delay: Duration::from_secs(5),
    };
    let temp = must(TempPdf::create());
    let path = temp.path().to_path_buf();

    let started = Instant::now();
    let result = with_s3_timeout(Duration::from_millis(50), || async {
        source.download_to("raw-pdfs/slow.pdf", 1024, &path).await
    })
    .await;

    assert!(is_s3_timeout(&result), "llego {result:?}");
    assert!(started.elapsed() < Duration::from_secs(1));
    assert_eq!(calls.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn timeouts_concurrentes_no_agotan_hilos() {
    let calls = Arc::new(AtomicUsize::new(0));
    let mut handles = Vec::new();
    for _ in 0..16 {
        let source = SlowSource {
            calls: calls.clone(),
            delay: Duration::from_secs(5),
        };
        let temp = must(TempPdf::create());
        let path = temp.path().to_path_buf();
        handles.push(tokio::spawn(async move {
            let result = with_s3_timeout(Duration::from_millis(50), || async {
                source.download_to("raw-pdfs/slow.pdf", 1024, &path).await
            })
            .await;
            assert!(is_s3_timeout(&result), "llego {result:?}");
        }));
    }

    let started = Instant::now();
    for handle in handles {
        must(handle.await);
    }
    assert!(
        started.elapsed() < Duration::from_secs(2),
        "16 timeouts concurrentes no deben serializarse"
    );
    assert_eq!(calls.load(Ordering::SeqCst), 16);
}
