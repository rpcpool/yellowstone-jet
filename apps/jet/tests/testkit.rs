use {
    rand::Rng,
    std::net::{SocketAddr, TcpListener},
};

#[allow(dead_code)]
pub fn find_available_port() -> Option<u16> {
    let mut rng = rand::rng();

    for _ in 0..100 {
        // Try up to 100 times to find an open port
        let (begin, end) = (32_768, 60_000);
        let port = rng.random_range(begin..=end);
        let addr = SocketAddr::from(([127, 0, 0, 1], port));

        // Try to bind to the port; if successful, port is free
        if TcpListener::bind(addr).is_ok() {
            return Some(port);
        }
    }

    None // If no port found after 100 attempts, return None
}

#[allow(dead_code)]
pub fn generate_random_local_addr() -> SocketAddr {
    let port = find_available_port().expect("port");
    SocketAddr::new("127.0.0.1".parse().expect("ipv4"), port)
}
