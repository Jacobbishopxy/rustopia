import { ProFormList, ProFormGroup, ProFormText, ProFormRadio } from "@ant-design/pro-form";
import { Space } from "antd";

const orderItem = [
    { label: 'Asc (升序)', value: 'Asc' },
    { label: 'Desc (降序)', value: 'Desc' },
]
export const OrderInputItem = () => {
    return (
        <ProFormList
            name="order"
            label="排序"
            itemRender={({ listDom, action }) => {
                return (
                    <>

                        <Space direction="horizontal">
                            {listDom}
                            {action}
                        </Space>
                    </>
                );
            }}
        >
            <ProFormGroup>
                <ProFormText name="name" label="name" rules={[{ required: true }]} />
                <ProFormRadio.Group
                    name="order"
                    options={orderItem}
                    label="升降序"
                />
            </ProFormGroup>
        </ProFormList>
    )
}