//! Conexion Redis Sentinel-aware (T-07) con re-resolucion ante failover
//! (T-08).
//!
//! Modo directo (`REDIS_SENTINEL_ADDRS` vacio): `redis::Client` sobre
//! `REDIS_URL`, igual que antes. Modo Sentinel: resuelve el master via los
//! sentinels configurados (`SentinelServerType::Master`, p.ej. `myprimary`)
//! y devuelve un `Client` apuntando a esa instancia descubierta.
//!
//! `RedisClient` envuelve el `redis::Client` actual tras un candado y
//! expone `reset()` para re-resolver el master cuando el viejo queda
//! degradado a replica (failover): las escrituras sobre la IP vieja
//! responden `READONLY` (XACK/XADD/XAUTOCLAIM/script Lua de la DLQ).
//!
//! Ningun mensaje de error de este modulo incluye `redis_url` ni passwords.

use std::sync::{Arc, RwLock};

use redis::aio::MultiplexedConnection;
use redis::{AsyncConnectionConfig, IntoConnectionInfo};

use super::config::{parse_sentinel_addr, ConsumerConfig};

/// Construye el `redis::Client` del worker segun la configuracion.
///
/// - Ambas claves sentinel vacias => modo directo sobre `cfg.redis_url`.
/// - Ambas presentes (tras [`ConsumerConfig::validate`]) => modo Sentinel:
///   los sentinels devuelven el master actual, nunca una IP fija.
///
/// La validacion corre primero: una config invalida falla aqui (y en el
/// runner) sin I/O y sin volcar credenciales en el mensaje.
pub fn build_client(cfg: &ConsumerConfig) -> redis::RedisResult<redis::Client> {
    cfg.validate().map_err(|e| {
        redis::RedisError::from((redis::ErrorKind::InvalidClientConfig, "config invalida", e))
    })?;
    if cfg.redis_sentinel_addrs.is_empty() {
        return redis::Client::open(cfg.redis_url.as_str());
    }
    build_sentinel_client(cfg)
}

/// `true` si el error del servidor es `READONLY` (escritura contra un
/// master degradado a replica tras un failover).
pub fn is_readonly(e: &redis::RedisError) -> bool {
    matches!(
        e.kind(),
        redis::ErrorKind::Server(redis::ServerErrorKind::ReadOnly)
    )
}

/// `true` si el error indica que el master cambio (`READONLY` o
/// `MASTERDOWN`): la conexion actual apunta a una instancia que ya no es
/// el master y hay que re-resolver via la fabrica.
pub fn is_failover(e: &redis::RedisError) -> bool {
    matches!(
        e.kind(),
        redis::ErrorKind::Server(
            redis::ServerErrorKind::ReadOnly | redis::ServerErrorKind::MasterDown
        )
    )
}

/// Cliente re-resoluble (T-08): conserva la config y el `redis::Client`
/// actual atras de un candado. Ante un failover, `reset()` re-ejecuta la
/// fabrica (`build_client`) y descarta el client viejo; las conexiones
/// nuevas salen ya contra el master vigente.
pub struct RedisClient {
    cfg: ConsumerConfig,
    client: RwLock<redis::Client>,
}

impl RedisClient {
    /// Valida la config y construye el client inicial (misma fabrica que
    /// `build_client`: modo directo sin I/O; modo Sentinel consulta los
    /// sentinels una vez).
    pub fn new(cfg: &ConsumerConfig) -> redis::RedisResult<Self> {
        Ok(Self {
            cfg: cfg.clone(),
            client: RwLock::new(build_client(cfg)?),
        })
    }

    /// `true` cuando la config apunta a Sentinel (re-resolucion activa).
    pub fn is_sentinel(&self) -> bool {
        !self.cfg.redis_sentinel_addrs.is_empty()
    }

    /// Re-resuelve el master via la fabrica y reemplaza el client interno,
    /// descartando el master viejo. En modo directo reconstruye el mismo
    /// `Client::open` (idempotente, sin I/O); en modo Sentinel vuelve a
    /// consultar a los sentinels. Los errores jamas incluyen credenciales.
    pub fn reset(&self) -> redis::RedisResult<()> {
        let fresh = build_client(&self.cfg)?;
        let mut guard = match self.client.write() {
            Ok(g) => g,
            Err(poisoned) => poisoned.into_inner(),
        };
        *guard = fresh;
        Ok(())
    }

    /// `reset()` en un hilo bloqueante: en modo Sentinel la fabrica hace
    /// I/O sincrona contra los sentinels y no debe correr en el runtime.
    pub async fn reset_async(self: &Arc<Self>) -> redis::RedisResult<()> {
        let client = Arc::clone(self);
        tokio::task::spawn_blocking(move || client.reset())
            .await
            .map_err(|e| {
                redis::RedisError::from((redis::ErrorKind::Io, "reset join", e.to_string()))
            })?
    }

    /// Conexion multiplexada al master actual (el `Client` vivo tras el
    /// candado; abre socket y hace el handshake de setup).
    pub async fn connect(&self) -> redis::RedisResult<MultiplexedConnection> {
        self.current().get_multiplexed_async_connection().await
    }

    /// Conexion EXCLUSIVA para el read bloqueante (`XREADGROUP BLOCK`):
    /// sin `response_timeout`, que el default de 500 ms siempre vence
    /// contra `block_ms` (defecto medido en T-04).
    pub async fn connect_read(&self) -> redis::RedisResult<MultiplexedConnection> {
        let cfg = AsyncConnectionConfig::new().set_response_timeout(None);
        self.current()
            .get_multiplexed_async_connection_with_config(&cfg)
            .await
    }

    fn current(&self) -> redis::Client {
        match self.client.read() {
            Ok(guard) => guard.clone(),
            Err(poisoned) => poisoned.into_inner().clone(),
        }
    }
}

fn build_sentinel_client(cfg: &ConsumerConfig) -> redis::RedisResult<redis::Client> {
    use redis::sentinel::{SentinelClientBuilder, SentinelServerType};

    // `validate()` ya garantiza que las addrs parsean y que el master name
    // existe; se re-parsean aqui para armar `ConnectionAddr` sin unwrap.
    let mut addrs = Vec::with_capacity(cfg.redis_sentinel_addrs.len());
    for raw in &cfg.redis_sentinel_addrs {
        let (host, port) = parse_sentinel_addr(raw).map_err(|e| {
            redis::RedisError::from((redis::ErrorKind::InvalidClientConfig, "addr invalida", e))
        })?;
        addrs.push(redis::ConnectionAddr::Tcp(host, port));
    }
    let master_name = match &cfg.redis_master_name {
        Some(name) => name.as_str(),
        None => {
            return Err(redis::RedisError::from((
                redis::ErrorKind::InvalidClientConfig,
                "config invalida",
                "REDIS_MASTER_NAME ausente".to_string(),
            )))
        }
    };

    // La auth del master viaja SOLO embebida en REDIS_URL; se extrae con la
    // API de redis (nunca con un parser manual). Sin password en la URL, no se
    // configura auth del master.
    let url_info = cfg.redis_url.as_str().into_connection_info()?;
    let master_password = url_info.redis_settings().password().map(str::to_owned);

    let mut builder = SentinelClientBuilder::new(addrs, master_name, SentinelServerType::Master)?;
    if let Some(password) = master_password.as_deref() {
        builder = builder.set_client_to_redis_password(password);
    }
    // T-10: la auth hacia los Sentinels es opt-in y NUNCA reusa el password
    // del master. Este stack corre Sentinels sin `requirepass`: mandarles
    // `AUTH` los rechaza y redis-rs lo reporta como `AuthenticationFailed`.
    if let Some(sentinel_password) = sentinel_auth_password(cfg) {
        builder = builder.set_client_to_sentinel_password(sentinel_password);
    }
    // `get_client` consulta a los sentinels (I/O) y devuelve un Client
    // apuntando al master que respondan: no hay IP fija en el codigo.
    let mut client = builder.build()?;
    client.get_client()
}

/// Password de auth hacia los Sentinels (T-10). Opt-in: solo
/// `REDIS_SENTINEL_PASSWORD`. No cae al password de `REDIS_URL` (master).
fn sentinel_auth_password(cfg: &ConsumerConfig) -> Option<&str> {
    cfg.redis_sentinel_password.as_deref()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::consumer::config::test_env;
    use std::time::Duration;

    /// Config de modo directo construida sin claves sentinel en el entorno.
    fn direct_config() -> ConsumerConfig {
        let _guard = test_env::lock();
        std::env::remove_var("REDIS_SENTINEL_ADDRS");
        std::env::remove_var("REDIS_MASTER_NAME");
        std::env::remove_var("REDIS_SENTINEL_PASSWORD");
        ConsumerConfig::from_env()
    }

    #[test]
    fn build_client_modo_directo_ok() {
        let cfg = direct_config();
        assert!(cfg.validate().is_ok());
        // `Client::open` solo parsea la URL: no abre socket, no necesita Redis.
        assert!(build_client(&cfg).is_ok());
    }

    #[test]
    fn build_client_modo_directo_con_password_ok() {
        let _guard = test_env::lock();
        std::env::set_var("REDIS_URL", "redis://:secreto123@127.0.0.1:6379/");
        std::env::remove_var("REDIS_SENTINEL_ADDRS");
        std::env::remove_var("REDIS_MASTER_NAME");
        let cfg = ConsumerConfig::from_env();
        assert!(
            build_client(&cfg).is_ok(),
            "una URL con password en modo directo sigue siendo valida"
        );
        std::env::remove_var("REDIS_URL");
    }

    #[test]
    fn build_client_config_invalida_falla_sin_credenciales() {
        let _guard = test_env::lock();
        std::env::set_var("REDIS_URL", "redis://:secreto123@127.0.0.1:6379/");
        std::env::set_var("REDIS_SENTINEL_ADDRS", "sentinel-1:26379");
        std::env::remove_var("REDIS_MASTER_NAME");
        let cfg = ConsumerConfig::from_env();
        let err = match build_client(&cfg) {
            Err(e) => e.to_string(),
            Ok(_) => panic!("addrs sin master name debe fallar antes de conectarse"),
        };
        assert!(
            !err.contains("secreto123"),
            "el error no puede contener el password: {err}"
        );
        assert!(
            !err.contains("redis://"),
            "el error no puede contener la URL: {err}"
        );
        std::env::remove_var("REDIS_URL");
    }

    // --- T-08: deteccion de failover ---

    fn err_readonly() -> redis::RedisError {
        redis::RedisError::from((
            redis::ErrorKind::Server(redis::ServerErrorKind::ReadOnly),
            "READONLY You can't write against a read only replica.",
        ))
    }

    fn err_master_down() -> redis::RedisError {
        redis::RedisError::from((
            redis::ErrorKind::Server(redis::ServerErrorKind::MasterDown),
            "MASTERDOWN Link with MASTER is down",
        ))
    }

    #[test]
    fn is_readonly_solo_marca_errores_readonly() {
        assert!(is_readonly(&err_readonly()));
        assert!(!is_readonly(&err_master_down()));
        assert!(!is_readonly(&redis::RedisError::from((
            redis::ErrorKind::Io,
            "connection reset"
        ))));
    }

    #[test]
    fn is_failover_marca_readonly_y_masterdown() {
        assert!(is_failover(&err_readonly()));
        assert!(is_failover(&err_master_down()));
        assert!(!is_failover(&redis::RedisError::from((
            redis::ErrorKind::Io,
            "connection reset"
        ))));
        assert!(!is_failover(&redis::RedisError::from((
            redis::ErrorKind::Server(redis::ServerErrorKind::ResponseError),
            "NOGROUP"
        ))));
    }

    // --- T-08: RedisClient en modo directo ---

    /// Config directo con `REDIS_URL` apuntando a `url` (bajo candado de env).
    fn direct_cfg_with_url(url: &str) -> ConsumerConfig {
        let _guard = test_env::lock();
        std::env::remove_var("REDIS_SENTINEL_ADDRS");
        std::env::remove_var("REDIS_MASTER_NAME");
        std::env::remove_var("REDIS_SENTINEL_PASSWORD");
        std::env::set_var("REDIS_URL", url);
        ConsumerConfig::from_env()
    }

    #[test]
    fn redis_client_directo_reset_es_idempotente_y_no_abre_socket() {
        let listener = std::net::TcpListener::bind("127.0.0.1:0")
            .unwrap_or_else(|e| panic!("bind de test: {e}"));
        listener
            .set_nonblocking(true)
            .unwrap_or_else(|e| panic!("nonblocking: {e}"));
        let addr = listener
            .local_addr()
            .unwrap_or_else(|e| panic!("local_addr: {e}"));
        let cfg = direct_cfg_with_url(&format!("redis://{addr}/"));

        let client = match RedisClient::new(&cfg) {
            Ok(c) => c,
            Err(e) => panic!("new en modo directo no debe fallar: {e}"),
        };
        assert!(!client.is_sentinel(), "modo directo no es sentinel");
        client
            .reset()
            .unwrap_or_else(|e| panic!("reset directo no debe fallar: {e}"));
        client
            .reset()
            .unwrap_or_else(|e| panic!("reset repetido debe ser idempotente: {e}"));

        // Ni `new` ni `reset` en modo directo abren sockets: `Client::open`
        // solo parsea la URL. Si alguno conectara, el accept lo veria.
        match listener.accept() {
            Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => {}
            Ok(_) => panic!("new/reset abrieron un socket contra el endpoint"),
            Err(e) => panic!("accept inesperado: {e}"),
        }
    }

    // --- T-10: la auth de Sentinel NO reusa el password del master ---

    #[test]
    fn sentinel_auth_solo_usa_redis_sentinel_password() {
        let mut cfg = direct_config();
        // REDIS_URL con password (auth del master). Sin REDIS_SENTINEL_PASSWORD
        // el worker no debe mandar AUTH a los Sentinels (stack sin requirepass).
        cfg.redis_url = "redis://:master-secret@127.0.0.1:6379/".to_string();
        cfg.redis_sentinel_password = None;
        assert_eq!(
            sentinel_auth_password(&cfg),
            None,
            "la auth de Sentinel no debe caer al password del master"
        );

        cfg.redis_sentinel_password = Some("sentinel-secret".to_string());
        assert_eq!(
            sentinel_auth_password(&cfg),
            Some("sentinel-secret"),
            "con REDIS_SENTINEL_PASSWORD se configura auth hacia Sentinels"
        );
    }

    #[test]
    fn redis_client_is_sentinel_refleja_la_config() {
        // Modo directo via constructor publico.
        let direct = direct_cfg_with_url("redis://127.0.0.1:6379/");
        let client = match RedisClient::new(&direct) {
            Ok(c) => c,
            Err(e) => panic!("new directo: {e}"),
        };
        assert!(!client.is_sentinel());

        // Modo sentinel: el struct se arma a mano (mismo modulo: campos
        // visibles) para no disparar la I/O de la fabrica en un test unit.
        let mut cfg = direct;
        cfg.redis_sentinel_addrs = vec!["127.0.0.1:26379".to_string()];
        cfg.redis_master_name = Some("myprimary".to_string());
        let sentinel = RedisClient {
            cfg,
            client: RwLock::new(
                build_client(&direct_cfg_with_url("redis://127.0.0.1:6379/"))
                    .unwrap_or_else(|e| panic!("client base: {e}")),
            ),
        };
        assert!(sentinel.is_sentinel());
    }

    // --- T-08: connect (eager) contra un endpoint controlado ---

    /// Servidor RESP minimo en un hilo: cuenta aceptaciones y responde
    /// `+OK` de mas a cada escritura, suficiente para que el setup de
    /// `connect` (CLIENT SETINFO) complete sin un Redis real.
    fn spawn_ok_server() -> (std::net::SocketAddr, Arc<std::sync::atomic::AtomicUsize>) {
        use std::io::{Read, Write};
        let listener = std::net::TcpListener::bind("127.0.0.1:0")
            .unwrap_or_else(|e| panic!("bind server: {e}"));
        let addr = listener
            .local_addr()
            .unwrap_or_else(|e| panic!("local_addr: {e}"));
        let accepts = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let counter = Arc::clone(&accepts);
        std::thread::spawn(move || {
            for stream in listener.incoming() {
                let Ok(mut stream) = stream else { break };
                counter.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                std::thread::spawn(move || {
                    let mut buf = [0u8; 4096];
                    // Primera escritura del cliente: el pipeline de setup.
                    if let Ok(n) = stream.read(&mut buf) {
                        if n > 0 {
                            let _ = stream.write_all(
                                b"+OK\r\n+OK\r\n+OK\r\n+OK\r\n+OK\r\n+OK\r\n+OK\r\n+OK\r\n",
                            );
                        }
                    }
                    // Cualquier escritura posterior (p.ej. PING): +OK tambien.
                    loop {
                        match stream.read(&mut buf) {
                            Ok(0) | Err(_) => break,
                            Ok(_) => {
                                let _ = stream.write_all(b"+OK\r\n+OK\r\n");
                            }
                        }
                    }
                });
            }
        });
        (addr, accepts)
    }

    #[tokio::test]
    async fn redis_client_connect_abre_un_socket_al_endpoint() {
        let (addr, accepts) = spawn_ok_server();
        let cfg = direct_cfg_with_url(&format!("redis://{addr}/"));
        let client = match RedisClient::new(&cfg) {
            Ok(c) => c,
            Err(e) => panic!("new: {e}"),
        };
        let conn = match tokio::time::timeout(Duration::from_secs(3), client.connect()).await {
            Ok(Ok(c)) => c,
            Ok(Err(e)) => panic!("connect al servidor stub: {e}"),
            Err(_) => panic!("connect no completo en 3s"),
        };
        drop(conn);
        assert_eq!(
            accepts.load(std::sync::atomic::Ordering::SeqCst),
            1,
            "connect debe abrir exactamente un socket al endpoint configurado"
        );

        // connect_read usa el mismo endpoint con response_timeout desactivado.
        let read_conn =
            match tokio::time::timeout(Duration::from_secs(3), client.connect_read()).await {
                Ok(Ok(c)) => c,
                Ok(Err(e)) => panic!("connect_read al servidor stub: {e}"),
                Err(_) => panic!("connect_read no completo en 3s"),
            };
        drop(read_conn);
        assert_eq!(
            accepts.load(std::sync::atomic::Ordering::SeqCst),
            2,
            "connect_read debe abrir su propio socket"
        );
    }

    #[tokio::test]
    async fn redis_client_connect_a_puerto_cerrado_falla_sin_filtrar_credenciales() {
        // Puerto cerrado: bindeamos y soltamos para quedarnos con la addr.
        let listener =
            std::net::TcpListener::bind("127.0.0.1:0").unwrap_or_else(|e| panic!("bind: {e}"));
        let addr = listener
            .local_addr()
            .unwrap_or_else(|e| panic!("local_addr: {e}"));
        drop(listener);

        let cfg = direct_cfg_with_url(&format!("redis://:secreto123@{addr}/"));
        let client = match RedisClient::new(&cfg) {
            Ok(c) => c,
            Err(e) => panic!("new no debe fallar (solo parsea): {e}"),
        };
        let err = match tokio::time::timeout(Duration::from_secs(5), client.connect()).await {
            Ok(Err(e)) => e.to_string(),
            Ok(Ok(_)) => panic!("un puerto cerrado no puede conectarse"),
            Err(_) => panic!("connect a puerto cerrado debe fallar rapido"),
        };
        assert!(
            !err.contains("secreto123"),
            "el error no puede contener el password: {err}"
        );
        assert!(
            !err.contains("redis://"),
            "el error no puede contener la URL: {err}"
        );
    }
}
