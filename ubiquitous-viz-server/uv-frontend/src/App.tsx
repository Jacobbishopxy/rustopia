import './App.css'

import {Tabs} from "antd"

import {checkConnection, createConn, listConn, deleteConn, updateConn} from "./services"
import {DatabaseConfiguration, SelectionModalForm} from "./components"

//To Delete
import {tableNameEnum, columnNameEnum} from "./components/QuerySelector/temp"

function App() {
  return (
    <div className="App">
      <header className="App-header">
        Welcome
      </header>
      <div className="App-body" >
        <div style={{width: "100%", backgroundColor: "white"}}>
          <Tabs style={{margin: "10px"}}>
            <Tabs.TabPane tab="Database Configation" key="db_config">
              <DatabaseConfiguration
                checkConnection={checkConnection}
                listConn={listConn}
                createConn={createConn}
                updateConn={updateConn}
                deleteConn={deleteConn}
              />
            </Tabs.TabPane>
            <Tabs.TabPane tab="Selection" key="select">
              <SelectionModalForm key='select' tableNameEnum={tableNameEnum} columnNameEnum={columnNameEnum} />
            </Tabs.TabPane>
          </Tabs>
        </div>
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
