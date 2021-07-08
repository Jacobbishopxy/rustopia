import {Space, Button, Tooltip} from "antd"
import type {ProColumns} from "@ant-design/pro-table"
import ProTable from "@ant-design/pro-table"
import {InfoCircleOutlined} from "@ant-design/icons"
import {useEffect, useState} from "react"


const columns: ProColumns<API.ConnInfo>[] = [
    {
        dataIndex: "name",
        title: "Name",
        render: (dom, record) => (
            <Space>
                <span>{dom}</span>
                <Tooltip title={record.id}>
                    <InfoCircleOutlined />
                </Tooltip>
            </Space>
        )
    },
    {
        dataIndex: "description",
        title: "Description",
    },
    {
        dataIndex: "driver",
        title: "Driver",
    },
    {
        dataIndex: "username",
        title: "Username",
    },
    {
        dataIndex: "password",
        title: "Password",
    },
    {
        dataIndex: "host",
        title: "Host",
    },
    {
        dataIndex: "port",
        title: "Port",
    },
    {
        dataIndex: "database",
        title: "Database",
    },
    {
        title: "Operation",
        valueType: "option",
        render: (_, record) => {
            return [
                <Button type="link">Edit</Button>,
                <Button type="link">Check</Button>,
                <Button type="link" danger>Remove</Button>,
            ]
        }
    }

]

export interface DatabaseConfigurationProps {
    // 
    checkConnection: (conn: API.ConnInfo) => Promise<boolean>
    // 
    listConn: () => Promise<API.ConnInfo[]>
    // 
    createConn: (conn: API.ConnInfo) => Promise<void>
    // 
    updateConn: (conn: API.ConnInfo) => Promise<void>
    // 
    deleteConn: (id: string) => Promise<void>
}

export const DatabaseConfiguration = (props: DatabaseConfigurationProps) => {
    const {listConn} = props
    const [data, setData] = useState<API.ConnInfo[]>()

    // fetch once
    useEffect(() => {
        listConn().then(res => {
            setData(res)
        })
    }, [listConn])


    return (
        <ProTable<API.ConnInfo>
            columns={columns}
            request={(params, sorter, filter) => {
                console.log(params, sorter, filter)
                return Promise.resolve({
                    data,
                    success: true,
                })
            }}
            rowKey="id"
            pagination={{showQuickJumper: true}}
            toolBarRender={false}
            search={false}
        />
    )
}

export default DatabaseConfiguration
