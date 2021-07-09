import {DatabaseConfiguration} from "./components"

import {checkConnection, createConn, listConn, deleteConn, updateConn} from "./services"

import './App.css'


function App() {
  return (
    <div className="App">
      <header className="App-header">
        Welcome
      </header>
      <div className="App-body">
        <DatabaseConfiguration
          checkConnection={checkConnection}
          listConn={listConn}
          createConn={createConn}
          updateConn={updateConn}
          deleteConn={deleteConn}
        />
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
