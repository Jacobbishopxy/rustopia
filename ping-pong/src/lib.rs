use tonic::{Request, Response};

pub mod pinger {
    tonic::include_proto!("ping");
}

pub use pinger::pinger_client::PingerClient;
pub use pinger::pinger_server::{Pinger, PingerServer};
use pinger::Resp;

#[derive(Debug, Default)]
pub struct PinGoose {}

#[tonic::async_trait]
impl Pinger for PinGoose {
    // 返回服务器时间
    async fn ping(&self, _: Request<()>) -> Result<Response<Resp>, tonic::Status> {
        let now = chrono::Utc::now().format("%b %-d, %-I:%M").to_string();

        let reply = pinger::Resp { resp: now };

        Ok(Response::new(reply))
    }

    // 无返回
    async fn ping_simple(
        &self,
        _: tonic::Request<()>,
    ) -> Result<tonic::Response<()>, tonic::Status> {
        Ok(Response::new(()))
    }
}
