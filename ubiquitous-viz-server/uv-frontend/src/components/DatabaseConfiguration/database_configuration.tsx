import {Button, Tooltip, Modal, message} from "antd"
import type {ProColumns} from "@ant-design/pro-table"
import ProTable from "@ant-design/pro-table"

import {DatabaseModalForm} from "./database_modal_form"
import {useState} from "react"

const columnsFactory = (
    onCheck: (connInfo: API.ConnInfo) => Promise<boolean>,
    onUpdate: (id: string, connInfo: API.ConnInfo) => Promise<void>,
    onDelete: (id: string) => void,
): ProColumns<API.ConnInfo>[] => {

    return [
        {
            dataIndex: "name",
            title: "Name",
            render: (dom, record) => (
                <Tooltip title={record.id}>
                    <span style={{textDecoration: "underline"}}>{dom}</span>
                </Tooltip>
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
                    <DatabaseModalForm
                        key="edit"
                        initValues={record}
                        trigger={<Button type="link">Edit</Button>}
                        onSubmit={d => onUpdate(record.id!, d)}
                    />,
                    <Button
                        key="check"
                        type="link"
                        onClick={async () => {
                            let res = await onCheck(record)
                            if (res) {
                                message.success("Connection succeed!")
                            } else {
                                message.error("Connection failed!")
                            }
                        }}
                    >
                        Check
                    </Button>,
                    <Button
                        key="delete"
                        type="link"
                        danger
                        onClick={() =>
                            Modal.warning({
                                content: "Are you sure to delete?",
                                onOk: async () => {
                                    return onDelete(record.id!)
                                }
                            })
                        }>
                        Delete
                    </Button>,
                ]
            }
        }
    ]
}

export interface DatabaseConfigurationProps {
    // check if database is connected
    checkConnection: (conn: API.ConnInfo) => Promise<boolean>
    // list all connection info
    listConn: () => Promise<API.ConnInfo[]>
    // create a new connection
    createConn: (conn: API.ConnInfo) => Promise<void>
    // update an existing connection
    updateConn: (id: string, conn: API.ConnInfo) => Promise<void>
    // delete an existing connection
    deleteConn: (id: string) => Promise<void>
}

export const DatabaseConfiguration = (props: DatabaseConfigurationProps) => {

    const [shouldRefresh, setShouldRefresh] = useState(0)

    const createConn = async (conn: API.ConnInfo) => {
        await props.createConn(conn)
        setShouldRefresh(shouldRefresh + 1)
    }

    const updateConn = async (id: string, conn: API.ConnInfo) => {
        await props.updateConn(id, conn)
        setShouldRefresh(shouldRefresh + 1)
    }

    const deleteConn = async (id: string) => {
        await props.deleteConn(id)
        setShouldRefresh(shouldRefresh + 1)
    }

    return (
        <ProTable<API.ConnInfo>
            rowKey="id"
            columns={columnsFactory(props.checkConnection, updateConn, deleteConn)}
            params={{shouldRefresh}}
            request={async (params, sorter, filter) => {
                const data = await props.listConn()
                return Promise.resolve({
                    data,
                    success: true,
                })
            }}
            toolbar={{
                title: "Database connection",
                actions: [
                    <DatabaseModalForm
                        key="create"
                        trigger={<Button type="link">New</Button>}
                        onSubmit={createConn}
                    />
                ]
            }}
            options={false}
            search={false}
        />
    )
}

export default DatabaseConfiguration
