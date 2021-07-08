import {Popconfirm, Space, Menu, Dropdown, Button} from "antd"
import type {ProColumns} from "@ant-design/pro-table"
import ProTable from "@ant-design/pro-table"
import {DownOutlined} from "@ant-design/icons"


export type Member = {
    realName: string
    nickName: string
    email: string
    outUserNo: string
    phone: string
    role: RoleType
    permission?: string[]
}

export type RoleMapType = Record<
    string,
    {
        name: string,
        desc: string
    }
>

export type RoleType = "admin" | "operator"

const RoleMap: RoleMapType = {
    admin: {
        name: "Administrator",
        desc: "admin"
    },
    operator: {
        name: "Operator",
        desc: "op"
    },
}

const tableListDataSource: Member[] = []

const realNames = ['Adam', 'Jav', 'Leo', 'Mia']
const nickNames = ['ad', 'jv', 'l', 'm']
const emails = ['ad@e.com', 'jv@e.com', 'l@e.com', 'mia@e.com']
const phones = ['123', '122', '111', '121']
const permissions = [[], ['p1', 'p4'], ['p1'], []]

for (let i = 0; i < 5; i += 1) {
    tableListDataSource.push({
        outUserNo: `${102047 + i}`,
        role: i === 0 ? 'admin' : 'operator',
        realName: realNames[i % 4],
        nickName: nickNames[i % 4],
        email: emails[i % 4],
        phone: phones[i % 4],
        permission: permissions[i % 4],
    })
}

const roleMenu = (
    <Menu>
        <Menu.Item key="admin">Administrator</Menu.Item>
        <Menu.Item key="operator">Operator</Menu.Item>
    </Menu>
)

export const DatabaseConfiguration = () => {
    const renderRemoveUser = (text: string) => (
        <Popconfirm key="popconfirm" title={`Confirm to ${text}`}>
            <Button type="link">{text}</Button>
        </Popconfirm>
    )

    const columns: ProColumns<Member>[] = [
        {
            dataIndex: "realName",
            title: "Name",
            width: 150,
            render: (dom, record) => (
                <Space>
                    <span>{dom}</span>
                    {record.nickName}
                </Space>
            )
        },
        {
            dataIndex: "email",
            title: "Account"
        },
        {
            dataIndex: "phone",
            title: "Phone number"
        },
        {
            dataIndex: "role",
            title: "Role",
            render: (_, record) => (
                <Dropdown overlay={roleMenu}>
                    <span>
                        {RoleMap[record.role || "admin"].name} <DownOutlined />
                    </span>
                </Dropdown>
            )
        },
        {
            dataIndex: "permission",
            title: "Permission",
            render: (_, record) => {
                const {role, permission = []} = record
                if (role === "admin") {
                    return "All rights"
                }
                return permission && permission.length > 0 ? permission.join(", ") : "None"
            }
        },
        {
            title: "operation",
            valueType: "option",
            render: (_, record) => {
                let node = renderRemoveUser("exit")
                if (record.role === "admin") {
                    node = renderRemoveUser("remove")
                }
                return [
                    <Button type="link" key="edit">Edit</Button>,
                    node
                ]
            }
        }
    ]

    return (
        <ProTable<Member>
            columns={columns}
            request={(params, sorter, filter) => {
                console.log(params, sorter, filter)
                return Promise.resolve({
                    data: tableListDataSource,
                    success: true,
                })
            }}
            rowKey="outUserNo"
            pagination={{showQuickJumper: true}}
            toolBarRender={false}
            search={false}
        />
    )
}

export default DatabaseConfiguration
