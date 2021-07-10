//! # Uv-backend
//!
//! Must used with `ubiquitous-alchemy-server`, in other words, UAS must
//! be started up in order to support this server.
//!
//! Supports `dev` and `prod` mode.
//!
//! The former mode is especially useful for frontend developing, since we
//! can modify frontend code without restarting itself by using `yarn start`.
//! In this mode, server provides CORS ability for the frontend, and finally
//! proxy all the requests to the API server.
//!
//! The latter mode is for production. It serves frontend codes as static files.

pub mod constant;
pub mod frontend;
pub mod proxy_agent;
pub mod util;
