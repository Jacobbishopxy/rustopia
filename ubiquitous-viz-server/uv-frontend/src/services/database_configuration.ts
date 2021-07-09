import axios from "axios"

export const checkConnection = async (param: API.ConnInfo): Promise<boolean> => {
    return axios.post("/cfg/check_connection", param).then((res: {data: boolean}) => res.data)
}

export const listConn = async (): Promise<API.ConnInfo[]> => {
    return axios.get("/cfg/conn").then((res: {data: API.ConnInfo[]}) => res.data)
}

export const createConn = async (param: API.ConnInfo): Promise<void> => {
    return axios.post("/cfg/conn", param)
}

export const updateConn = async (id: string, param: API.ConnInfo): Promise<void> => {
    return axios.put(`/cfg/conn?db_id=${id}`, param)
}

export const deleteConn = async (id: string): Promise<void> => {
    return axios.delete(`/cfg/conn?db_id=${id}`)
}
