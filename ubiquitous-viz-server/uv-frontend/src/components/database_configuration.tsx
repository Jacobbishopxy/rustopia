import {Space, Button, Tooltip, Modal, message} from "antd"
import type {ProColumns} from "@ant-design/pro-table"
import ProTable from "@ant-design/pro-table"
import {InfoCircleOutlined} from "@ant-design/icons"

import {DatabaseForm} from "./database_form"

const columnsFactory = (
    onCheck: (connInfo: API.ConnInfo) => Promise<boolean>,
    onUpdate: (connInfo: API.ConnInfo) => Promise<void>,
    onRemove: (id: string) => void,
): ProColumns<API.ConnInfo>[] => {

    return [
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
                    <DatabaseForm
                        key="edit"
                        isCreate={false}
                        trigger={
                            <Button type="link">Edit</Button>
                        }
                        onSubmit={onUpdate}
                    />,
                    <Button
                        key="check"
                        type="link"
                        onClick={async () => {
                            console.log(record)
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
                        key="remove"
                        type="link"
                        danger
                        onClick={() =>
                            Modal.warning({
                                content: "Are you sure to delete?",
                                onOk: async () => {
                                    return onRemove(record.id!)
                                }
                            })
                        }>
                        Remove
                    </Button>,
                ]
            }
        }
    ]
}

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

    return (
        <ProTable<API.ConnInfo>
            columns={columnsFactory(props.checkConnection, props.createConn, props.deleteConn)}
            request={async (params, sorter, filter) => {
                const data = await props.listConn()
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
