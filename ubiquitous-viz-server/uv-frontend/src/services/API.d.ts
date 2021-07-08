declare namespace API {
    interface ConnInfo {
        id?: string
        name?: string
        description?: string
        driver: string
        username: string
        password: string
        host: string
        port: number
        database: string
    }
}
