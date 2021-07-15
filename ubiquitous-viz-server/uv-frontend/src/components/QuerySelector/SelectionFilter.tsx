import {ProFormList, ProFormRadio, ProFormGroup} from "@ant-design/pro-form"
import {Space} from "antd"
import _ from "lodash"
import {ConditionItem} from "./SelectionConditionItem"

const conjunctionItem =
    [{
        label: 'AND (和)',
        value: 'AND',
    },
    {
        label: 'OR (或)',
        value: 'OR',
    }]


export const FilterItem = () => {
    return (
        <ProFormList
            name="filter"
            label="筛选条件(可选)"
            creatorButtonProps={{creatorButtonText: "添加一行条件"}}
            copyIconProps={{
                tooltipText: '复制此项到末尾',
            }}
            deleteIconProps={{
                tooltipText: '不需要这项了',
            }}
            itemRender={({listDom, action}, {field, fields}) => {

                const conjunction = field.fieldKey !== _.first(fields)?.fieldKey ?
                    <ProFormRadio.Group
                        rules={[{required: true}]}
                        name="junction"
                        label="上一项与该项的逻辑关系"
                        options={conjunctionItem}
                    /> : <></>

                return (
                    <Space direction="vertical">
                        {conjunction}
                        {listDom}
                        {action}
                    </Space>
                )
            }}
        >
            <ConditionItem />
            <div style={{width: "80%", marginInlineStart: "10%", marginBlockEnd: "5%", border: "1px  lightgrey", padding: "10px"}}>

                <ProFormList
                    name="filter"
                    label="内嵌条件(可选)"
                    creatorButtonProps={{creatorButtonText: "添加一行内嵌条件"}}
                    itemRender={({listDom, action}) => {
                        const conjunction =
                            <ProFormRadio.Group
                                rules={[{required: true}]}
                                name="junction"
                                label="上一条件与该条件的逻辑关系"
                                options={conjunctionItem}
                            />

                        return (
                            <>
                                {conjunction}
                                <Space direction="horizontal">
                                    {listDom}
                                    {action}
                                </Space>
                            </>
                        )
                    }}
                >
                    <ProFormGroup>
                        <ConditionItem />
                    </ProFormGroup>
                </ProFormList>
            </div>
        </ProFormList>
    )
}