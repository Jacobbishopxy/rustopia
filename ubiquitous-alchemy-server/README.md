# Ubiquitous Alchemy Server

## 依赖

1. 由[actix-web](https://github.com/actix/actix-web)提供的 HTTP 服务接受外部请求
1. 通过[serde](https://github.com/serde-rs/serde)解析 JSON
1. 使用[sea-query](https://github.com/SeaQL/sea-query)生成 raw sql 字符串
1. 通过[sqlx](https://github.com/launchbadge/sqlx)创建数据库连接池，并与数据库进行交互

## 结构

1. ua-dao
1. ua-model
1. ua-service
