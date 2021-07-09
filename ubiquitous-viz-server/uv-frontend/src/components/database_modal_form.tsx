import {ModalForm, ProFormDigit, ProFormSelect, ProFormText} from "@ant-design/pro-form"

export interface DatabaseModalFormProps {
    initValues?: API.ConnInfo
    trigger: JSX.Element
    onSubmit: (data: API.ConnInfo) => Promise<void>
}

export const DatabaseModalForm = (props: DatabaseModalFormProps) => {

    const onSubmit = async (data: API.ConnInfo) => {
        if (props.initValues) {
            await props.onSubmit({...data, id: props.initValues.id})
        } else {
            await props.onSubmit(data)
        }
        return true
    }

    return (
        <ModalForm
            title="Database connection"
            initialValues={props.initValues}
            trigger={props.trigger}
            onFinish={onSubmit}
        >
            <ProFormText
                name="name"
                label="Name"
                placeholder="Please enter a name"
                rules={[{required: true, message: "Please enter a name!"}]}
            />
            <ProFormText
                name="description"
                label="Description"
            />
            <ProFormSelect
                name="driver"
                label="Driver"
                valueEnum={{
                    postgres: "Postgres",
                    mysql: "MySql"
                }}
                placeholder="Please select a driver"
                rules={[{required: true, message: "Please select a driver!"}]}
            />
            <ProFormText
                name="username"
                label="Username"
                placeholder="Please enter your username"
                rules={[{required: true, message: "Please enter your username!"}]}
            />
            <ProFormText.Password
                name="password"
                label="Name"
                placeholder="Please enter your password"
                rules={[{required: true, message: "Please enter your password!"}]}
            />
            <ProFormText
                name="host"
                label="Host"
                placeholder="Please enter database host"
                rules={[{required: true, message: "Please enter database host!"}]}
            />
            <ProFormDigit
                name="port"
                label="Port"
                min={0}
                fieldProps={{precision: 0}}
                rules={[{required: true, message: "Please enter database port!"}]}
            />
            <ProFormText
                name="database"
                label="Database"
                placeholder="Please enter database name"
                rules={[{required: true, message: "Please enter database name!"}]}
            />
        </ModalForm>
    )
}

export default DatabaseModalForm
