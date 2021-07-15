import { ProFormGroup, ProFormText, ProFormSelect, ProFormDependency } from "@ant-design/pro-form"

enum conditionEnum {
    Equal = "=",
    NotEqual = "!=",
    Greater = ">",
    GreaterEqual = ">=",
    Less = "<",
    LessEqual = "<=",
    In = "in",
    Between = "between",
    Like = "like",
}
//helper method to dynamically generate Form item based on different conditions
const ConditionToDataItem = (condition: string) => {
    // console.log(condition)
    switch (condition) {
        case "Between":

            return (
                <ProFormGroup >
                    <ProFormText
                        name="Min"
                        label="Min"
                        width={75}
                        allowClear={false}
                        rules={[{ required: true }]}
                    />
                    <ProFormText
                        name="Max"
                        label="Max"
                        width={75}
                        allowClear={false}
                        rules={[{ required: true }]}
                    />
                </ProFormGroup>

            )
        case "In":
            return (
                <ProFormSelect
                    label="Value matches"
                    rules={[{ required: true }]}
                    mode="tags"
                    placeholder="请输入"
                    name={condition}
                />
            )
    }
    return (
        <ProFormText
            width="md"
            rules={[{ required: true }]}
            // name={equation}
            label={`Value`}
            placeholder="请输入"
            name={condition}
        />
    );
}
//form group for condition item, with a select from ConditionEnum, and its' data input item
//for "Between" we have a pair; "In" we have a multi-input box; And all the remaining conditions used a text input box
export const ConditionItem = () => {
    return (
        <ProFormGroup >
            <ProFormText name="column" label="column name" rules={[{ required: true }]} />
            <ProFormSelect
                rules={[{ required: true }]}
                name="condition"
                label="condition"
                valueEnum={conditionEnum} />
            <ProFormDependency name={["condition"]}>
                {({ condition }) => {
                    // console.log(condition)
                    return ConditionToDataItem(condition)
                }}
            </ProFormDependency>
        </ProFormGroup>)
}