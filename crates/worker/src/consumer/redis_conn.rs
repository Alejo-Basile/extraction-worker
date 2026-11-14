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
use std::time::Duration;

use redis::aio::MultiplexedConnection;
use redis::{
    AsyncConnectionConfig, ConnectionAddr, ConnectionInfo, ErrorKind, IntoConnectionInfo,
    RedisConnectionInfo, RedisError,
};

use super::config::{parse_sentinel_addr, ConsumerConfig};

/// Cota por consulta a un Sentinel (conexion + comando). Un Sentinel que
/// acepta TCP pero no responde no puede colgar la re-resolucion del master.
const SENTINEL_QUERY_TIMEOUT: Duration = Duration::from_secs(2);

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
    resolve_master_client_sync(cfg)
}

/// Igual que [`build_client`] pero con re-resolucion async (timeouts) cuando
/// la config apunta a Sentinel. Es el camino del runtime: nunca bloquea el
/// reactor en I/O de Sentinels ni se cuelga contra un master pausado.
pub async fn build_client_async(cfg: &ConsumerConfig) -> redis::RedisResult<redis::Client> {
    cfg.validate().map_err(|e| {
        redis::RedisError::from((redis::ErrorKind::InvalidClientConfig, "config invalida", e))
    })?;
    if cfg.redis_sentinel_addrs.is_empty() {
        return redis::Client::open(cfg.redis_url.as_str());
    }
    resolve_master_client_async(cfg).await
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

    /// Constructor async: en modo Sentinel resuelve el master con timeouts y
    /// sin conectar al candidato. Usarlo en el runtime (runner y readyz).
    pub async fn new_async(cfg: &ConsumerConfig) -> redis::RedisResult<Self> {
        Ok(Self {
            cfg: cfg.clone(),
            client: RwLock::new(build_client_async(cfg).await?),
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

    /// `reset()` async: re-resuelve el master con timeouts y sin
    /// `spawn_blocking`. Un Sentinel que no responde devuelve error en
    /// `SENTINEL_QUERY_TIMEOUT`, no cuelga el reactor.
    pub async fn reset_async(self: &Arc<Self>) -> redis::RedisResult<()> {
        let fresh = build_client_async(&self.cfg).await?;
        let mut guard = match self.client.write() {
            Ok(g) => g,
            Err(poisoned) => poisoned.into_inner(),
        };
        *guard = fresh;
        Ok(())
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

/// Nombre del master configurado (`REDIS_MASTER_NAME`).
fn master_name(cfg: &ConsumerConfig) -> redis::RedisResult<&str> {
    cfg.redis_master_name.as_deref().ok_or_else(|| {
        RedisError::from((
            ErrorKind::InvalidClientConfig,
            "config invalida",
            "REDIS_MASTER_NAME ausente".to_string(),
        ))
    })
}

/// Addrs de los Sentinels ya parseadas. `validate()` corre antes, pero se
/// re-parsean para no depender de indices ni unwraps.
fn sentinel_targets(cfg: &ConsumerConfig) -> redis::RedisResult<Vec<(String, u16)>> {
    cfg.redis_sentinel_addrs
        .iter()
        .map(|raw| {
            parse_sentinel_addr(raw)
                .map_err(|e| RedisError::from((ErrorKind::InvalidClientConfig, "addr invalida", e)))
        })
        .collect()
}

/// `ConnectionInfo` de un Sentinel. La auth de Sentinel es opt-in
/// (`REDIS_SENTINEL_PASSWORD`) y nunca reusa el password del master.
fn sentinel_connection_info(
    host: &str,
    port: u16,
    cfg: &ConsumerConfig,
) -> redis::RedisResult<ConnectionInfo> {
    let base = format!("redis://{host}:{port}").into_connection_info()?;
    Ok(match sentinel_auth_password(cfg) {
        Some(password) => {
            base.set_redis_settings(RedisConnectionInfo::default().set_password(password))
        }
        None => base,
    })
}

/// `ConnectionInfo` del master: conserva password/db/username/protocol de
/// `REDIS_URL` y solo reemplaza la direccion por la que respondio Sentinel.
fn master_connection_info(
    host: &str,
    port: u16,
    cfg: &ConsumerConfig,
) -> redis::RedisResult<ConnectionInfo> {
    let base = cfg.redis_url.as_str().into_connection_info()?;
    Ok(base.set_addr(ConnectionAddr::Tcp(host.to_string(), port)))
}

fn client_from_master_reply(
    reply: &[String],
    cfg: &ConsumerConfig,
) -> redis::RedisResult<redis::Client> {
    if reply.len() != 2 {
        return Err(RedisError::from((
            ErrorKind::UnexpectedReturnType,
            "respuesta de SENTINEL invalida",
        )));
    }
    let port = reply[1].parse::<u16>().map_err(|_| {
        RedisError::from((
            ErrorKind::UnexpectedReturnType,
            "puerto de SENTINEL invalido",
        ))
    })?;
    // NUNCA se conecta al master aca: solo se construye el Client. Conectar
    // al candidato es lo que colgaba contra un master pausado.
    redis::Client::open(master_connection_info(&reply[0], port, cfg)?)
}

/// Resolucion sync (modo CLI/tests): consulta solo a los Sentinels, nunca
/// abre socket contra el master. Sin timeout por sentinel; el runtime usa
/// la version async.
fn resolve_master_client_sync(cfg: &ConsumerConfig) -> redis::RedisResult<redis::Client> {
    let name = master_name(cfg)?;
    let mut last_err: Option<RedisError> = None;
    for (host, port) in sentinel_targets(cfg)? {
        let attempt = (|| -> redis::RedisResult<redis::Client> {
            let client = redis::Client::open(sentinel_connection_info(&host, port, cfg)?)?;
            let mut conn = client.get_connection()?;
            let reply: Vec<String> = redis::cmd("SENTINEL")
                .arg("get-master-addr-by-name")
                .arg(name)
                .query(&mut conn)?;
            client_from_master_reply(&reply, cfg)
        })();
        match attempt {
            Ok(client) => return Ok(client),
            Err(e) => last_err = Some(e),
        }
    }
    Err(last_err.unwrap_or_else(|| {
        RedisError::from((
            ErrorKind::MasterNameNotFoundBySentinel,
            "ningun sentinel respondio",
        ))
    }))
}

/// Resolucion async con timeout por Sentinel (conexion + comando).
async fn resolve_master_client_async(cfg: &ConsumerConfig) -> redis::RedisResult<redis::Client> {
    let name = master_name(cfg)?;
    let mut last_err: Option<RedisError> = None;
    for (host, port) in sentinel_targets(cfg)? {
        match query_sentinel_master_async(&host, port, name, cfg).await {
            Ok((master_host, master_port)) => {
                return redis::Client::open(master_connection_info(&master_host, master_port, cfg)?)
            }
            Err(e) => last_err = Some(e),
        }
    }
    Err(last_err.unwrap_or_else(|| {
        RedisError::from((
            ErrorKind::MasterNameNotFoundBySentinel,
            "ningun sentinel respondio",
        ))
    }))
}

async fn query_sentinel_master_async(
    host: &str,
    port: u16,
    name: &str,
    cfg: &ConsumerConfig,
) -> redis::RedisResult<(String, u16)> {
    let client = redis::Client::open(sentinel_connection_info(host, port, cfg)?)?;
    let mut conn = tokio::time::timeout(
        SENTINEL_QUERY_TIMEOUT,
        client.get_multiplexed_async_connection(),
    )
    .await
    .map_err(|_| RedisError::from((ErrorKind::Io, "timeout conectando al Sentinel")))??;

    let reply: Vec<String> = tokio::time::timeout(
        SENTINEL_QUERY_TIMEOUT,
        redis::cmd("SENTINEL")
            .arg("get-master-addr-by-name")
            .arg(name)
            .query_async(&mut conn),
    )
    .await
    .map_err(|_| RedisError::from((ErrorKind::Io, "timeout consultando al Sentinel")))??;

    if reply.len() != 2 {
        return Err(RedisError::from((
            ErrorKind::UnexpectedReturnType,
            "respuesta de SENTINEL invalida",
        )));
    }
    let master_port = reply[1].parse::<u16>().map_err(|_| {
        RedisError::from((
            ErrorKind::UnexpectedReturnType,
            "puerto de SENTINEL invalido",
        ))
    })?;
    Ok((reply[0].clone(), master_port))
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

    // --- T-08 (S6-P1-01): resolucion Sentinel sin colgarse en el master ---

    /// Config Sentinel apuntando a los `addrs` dados.
    fn sentinel_cfg(addrs: Vec<String>) -> ConsumerConfig {
        let mut cfg = direct_cfg_with_url("redis://127.0.0.1:1/");
        cfg.redis_sentinel_addrs = addrs;
        cfg.redis_master_name = Some("myprimary".to_string());
        cfg
    }

    /// Servidor que acepta conexiones y **nunca responde** (master pausado).
    /// Cuenta las aceptaciones para probar quien se conecta.
    async fn spawn_black_hole() -> (std::net::SocketAddr, Arc<std::sync::atomic::AtomicUsize>) {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .unwrap_or_else(|e| panic!("bind black hole: {e}"));
        let addr = listener
            .local_addr()
            .unwrap_or_else(|e| panic!("local_addr: {e}"));
        let accepts = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let counter = Arc::clone(&accepts);
        tokio::spawn(async move {
            loop {
                let (stream, _) = match listener.accept().await {
                    Ok(v) => v,
                    Err(_) => break,
                };
                counter.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                tokio::spawn(async move {
                    // Mantener el socket abierto sin escribir jamas.
                    let _keep = stream;
                    tokio::time::sleep(Duration::from_secs(30)).await;
                });
            }
        });
        (addr, accepts)
    }

    /// Extrae un comando RESP array completo del buffer: `(args, consumidos)`.
    fn take_resp_command(buf: &[u8]) -> Option<(Vec<String>, usize)> {
        if buf.first() != Some(&b'*') {
            return None;
        }
        let end = buf.iter().position(|&b| b == b'\n')?;
        let n: usize = std::str::from_utf8(&buf[1..end])
            .ok()?
            .trim()
            .parse()
            .ok()?;
        let mut pos = end + 1;
        let mut args = Vec::with_capacity(n);
        for _ in 0..n {
            if buf.get(pos) != Some(&b'$') {
                return None;
            }
            let e = buf[pos..].iter().position(|&b| b == b'\n')? + pos;
            let len: usize = std::str::from_utf8(&buf[pos + 1..e])
                .ok()?
                .trim()
                .parse()
                .ok()?;
            let start = e + 1;
            let arg_end = start + len;
            if buf.len() < arg_end + 2 {
                return None;
            }
            args.push(String::from_utf8_lossy(&buf[start..arg_end]).to_string());
            pos = arg_end + 2;
        }
        Some((args, pos))
    }

    /// Sentinel RESP minimo: responde `+OK` al setup y la addr del master a
    /// `SENTINEL get-master-addr-by-name`.
    async fn spawn_sentinel_stub(master_port: u16) -> std::net::SocketAddr {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .unwrap_or_else(|e| panic!("bind sentinel: {e}"));
        let addr = listener
            .local_addr()
            .unwrap_or_else(|e| panic!("local_addr: {e}"));
        tokio::spawn(async move {
            loop {
                let (mut stream, _) = match listener.accept().await {
                    Ok(v) => v,
                    Err(_) => break,
                };
                tokio::spawn(async move {
                    let mut buf = Vec::new();
                    loop {
                        let mut chunk = [0u8; 512];
                        let n = match stream.read(&mut chunk).await {
                            Ok(0) | Err(_) => break,
                            Ok(n) => n,
                        };
                        buf.extend_from_slice(&chunk[..n]);
                        while let Some((args, consumed)) = take_resp_command(&buf) {
                            buf.drain(..consumed);
                            let is_sentinel = args
                                .first()
                                .map(|a| a.eq_ignore_ascii_case("SENTINEL"))
                                .unwrap_or(false);
                            let reply = if is_sentinel {
                                let port = master_port.to_string();
                                format!("*2\r\n$9\r\n127.0.0.1\r\n${}\r\n{}\r\n", port.len(), port)
                            } else {
                                "+OK\r\n".to_string()
                            };
                            if stream.write_all(reply.as_bytes()).await.is_err() {
                                return;
                            }
                        }
                    }
                });
            }
        });
        addr
    }

    #[tokio::test]
    async fn resolver_async_no_se_conecta_al_master() {
        let (master, accepts) = spawn_black_hole().await;
        let sentinel = spawn_sentinel_stub(master.port()).await;
        let cfg = sentinel_cfg(vec![sentinel.to_string()]);

        let started = std::time::Instant::now();
        let client =
            match tokio::time::timeout(Duration::from_secs(3), build_client_async(&cfg)).await {
                Ok(Ok(client)) => client,
                Ok(Err(e)) => panic!("resolver async: {e}"),
                Err(_) => panic!("el resolver no debe colgarse contra el master"),
            };
        assert!(
            started.elapsed() < Duration::from_secs(3),
            "el resolver debe terminar rapido"
        );
        assert_eq!(
            accepts.load(std::sync::atomic::Ordering::SeqCst),
            0,
            "el resolver NO debe abrir socket contra el master"
        );

        // El client resuelto apunta al master: recien el connect abre el socket.
        let _ = tokio::time::timeout(
            Duration::from_millis(700),
            client.get_multiplexed_async_connection(),
        )
        .await;
        assert_eq!(
            accepts.load(std::sync::atomic::Ordering::SeqCst),
            1,
            "connect posterior debe ir al master resuelto"
        );
    }

    #[tokio::test]
    async fn resolver_async_corta_con_sentinel_mudo() {
        let (silent, _) = spawn_black_hole().await;
        let cfg = sentinel_cfg(vec![silent.to_string()]);

        let started = std::time::Instant::now();
        let result =
            match tokio::time::timeout(Duration::from_secs(6), build_client_async(&cfg)).await {
                Ok(result) => result,
                Err(_) => panic!("el timeout interno por sentinel debe cortar antes"),
            };
        assert!(result.is_err(), "un sentinel mudo no puede resolver");
        assert!(
            started.elapsed() < Duration::from_secs(6),
            "debe cortar por SENTINEL_QUERY_TIMEOUT, no colgarse"
        );
    }
}
