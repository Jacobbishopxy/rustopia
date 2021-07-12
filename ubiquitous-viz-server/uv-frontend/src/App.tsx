import { DatabaseConfiguration } from "./components"

import { checkConnection, createConn, listConn, deleteConn, updateConn } from "./services"

import './App.css'
import SelectionModalForm from "./components/selection_modal_form"

//To Delete
import { tableNameEnum, columnNameEnum } from "./components/temp"

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
      <div className="App-body">
        <SelectionModalForm tableNameEnum={tableNameEnum} columnNameEnum={columnNameEnum} />
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
