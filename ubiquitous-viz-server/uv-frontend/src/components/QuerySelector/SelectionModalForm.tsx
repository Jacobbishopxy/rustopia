
import {ModalForm, ProFormDigit, ProFormGroup, ProFormSelect} from "@ant-design/pro-form"
import {Button} from "antd"
import {FilterItem} from "./SelectionFilter"
import {OrderInputItem} from "./SelectionOrder"


export interface SelectionModalFormProps {
    initValues?: API.Selection
    tableNameEnum: {[key: string]: string}
    columnNameEnum: {[key: string]: string}

    // trigger: JSX.Element
    // onSubmit: (data: API.Selection) => Promise<void>
}
interface cond extends API.Equation {
    junction?: API.Conjunction
    condition: API.conditionToString
    Min?: API.DataEnum
    Max?: API.DataEnum
    column: string

}
export const SelectionModalForm = (props: SelectionModalFormProps) => {

    //used to parse a single condition
    const parseSingleEquation = (condition: cond) => {
        // console.log(condition.condition)
        // let equationItem: API.Equation = condition.In
        switch (condition.condition) {
            case "In":
                console.log("in In")
                return {In: condition.In}
            case "Between":
                return {Between: [condition.Max, condition.Min]}
            case "Like":
                return {Like: condition.Like}
            case "LessEqual":
                return {LessEqual: condition.LessEqual}
            case "Equal":
                return {Equal: condition.Equal}
            case "Less":
                return {Less: condition.Less}
            case "Greater":
                return {Greater: condition.Greater}
            case "GreaterEqual":
                return {GreaterEqual: condition.GreaterEqual}
            case "NotEqual":
                return {NotEqual: condition.NotEqual}
        }

    }
    //used to parsed embedded filter condition
    const embedFilterParser = (filterArray: cond[]) => {
        // console.log(filterArray)
        let parsedArray: (API.Expression)[] = []
        for (let item of filterArray) {
            // console.log(item)
            parsedArray.push(item.junction as API.Conjunction)
            // console.log(parsedArray)
            parsedArray.push({equation: parseSingleEquation(item as cond), column: item.column} as API.Condition)
        }
        return parsedArray
    }
    //converter from form's value to api data type
    const converter = async (data: any) => {
        // console.log(data)
        let temp: (API.Expression)[] = []
        for (let item of data.filter) {
            console.log(item)
            //add conjunction
            if (item.junction) {
                temp.push(item.junction)
            }
            temp.push({column: item.column, equation: parseSingleEquation(item as cond) as API.Equation})
            //set up embeded condition
            if (item.filter) {
                // embedFilterParser(item.filter)
                temp.push(embedFilterParser(item.filter))
            }
        }
        // console.dir(temp)
        // console.log(parseSingleEquation(ele as unknown as condi))

        let selection: API.Selection = {
            ...data,
            filter: temp
        }
        //selection is the json type requred by API.Selection
        console.log(selection)
    }

    return (
        <ModalForm
            title="Selection"
            initialValues={props.initValues}
            // trigger={props.trigger}
            trigger={
                <Button type="primary">
                    Add a New Node
                </Button>
            }
            onFinish={converter}
        >
            <ProFormSelect
                name="table"
                label="table name"
                valueEnum={props.tableNameEnum}
                placeholder="Please select a table name"
                rules={[{required: true, message: "Please select a table name!"}]}
            />
            <ProFormSelect
                name="columns"
                label="column[s] name"
                mode="multiple"
                valueEnum={props.columnNameEnum}
                placeholder="Please select a column name"
                rules={[{required: true, message: "Please select a column name!"}]}
            />

            <FilterItem />

            <OrderInputItem />
            <ProFormGroup>
                <ProFormDigit
                    name="limit"
                    min={0}
                    label="Limit"
                />
                <ProFormDigit
                    name="offset"
                    min={0}
                    label="Offset"
                />
            </ProFormGroup>
        </ModalForm>
    )
}


export default SelectionModalForm
