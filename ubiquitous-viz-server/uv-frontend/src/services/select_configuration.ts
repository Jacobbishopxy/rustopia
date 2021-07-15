import axios from "axios"

export const select = async (id: string, selection: API.Selection): Promise<any[]> => {
    return axios.post(`/query/select?db_id=${id}`, selection)
}
