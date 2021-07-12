import { DatabaseConfiguration } from "./components"

import { checkConnection, createConn, listConn, deleteConn, updateConn } from "./services"

import './App.css'
import SelectionModalForm from "./components/selection_modal_form"

//To Delete
import { tableNameEnum, columnNameEnum } from "./components/temp"
import { Button, Tabs } from "antd"
import { PageContainer } from "@ant-design/pro-layout"
import TabPane from "@ant-design/pro-card/lib/components/TabPane"

function App() {
  return (
    <div className="App">
      <header className="App-header">
        Welcome
      </header>
      {/* <div className="App-body">
        <DatabaseConfiguration
          checkConnection={checkConnection}
          listConn={listConn}
          createConn={createConn}
          updateConn={updateConn}
          deleteConn={deleteConn}
        />
      </div> */}
      <div >
        <Tabs style={{ margin: "10px" }}>
          <TabPane tab="Database Configation" key="db_config">
            <div className="App-body" >
              <DatabaseConfiguration
                checkConnection={checkConnection}
                listConn={listConn}
                createConn={createConn}
                updateConn={updateConn}
                deleteConn={deleteConn}
              />
            </div>
          </TabPane>
          <TabPane tab="Selection" key="select">
            <div className="App-body" >
              <SelectionModalForm key='select' tableNameEnum={tableNameEnum} columnNameEnum={columnNameEnum} />
            </div>
          </TabPane>


        </Tabs>
        {/* <SelectionModalForm tableNameEnum={tableNameEnum} columnNameEnum={columnNameEnum} /> */}
      </div>
      <div className="App-footer">
        <a
          className="App-link"
          href="https://github.com/Jacobbishopxy/rustopia"
        >
          https://github.com/Jacobbishopxy/rustopia
        </a>
      </div>
    </div>
  )
}

export default App
