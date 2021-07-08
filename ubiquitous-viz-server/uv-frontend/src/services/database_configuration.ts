import axios from "axios"

export const checkConnection = async (param: API.ConnInfo): Promise<boolean> => {
    return axios.post("/api/cfg/check_connection", param).then((res: {data: boolean}) => res.data)
}

export const listConn = async (): Promise<API.ConnInfo[]> => {
    return axios.get("/api/cfg/conn").then((res: {data: API.ConnInfo[]}) => res.data)
}

export const createConn = async (param: API.ConnInfo): Promise<void> => {
    return axios.post("/api/cfg/conn", param)
}

export const updateConn = async (param: API.ConnInfo): Promise<void> => {
    return axios.put("/api/cfg/conn", param)
}

export const deleteConn = async (id: string): Promise<void> => {
    return axios.delete(`/api/cfg/conn?db_id=${id}`)
}
