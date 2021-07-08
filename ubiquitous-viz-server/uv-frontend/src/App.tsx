import {DatabaseConfiguration} from "./components"

import {checkConnection, createConn, listConn, deleteConn, updateConn} from "./services"

import './App.css'


function App() {
  return (
    <div className="App">
      <header className="App-header">
        <DatabaseConfiguration
          checkConnection={checkConnection}
          listConn={listConn}
          createConn={createConn}
          updateConn={updateConn}
          deleteConn={deleteConn}
        />
      </header>
    </div>
  )
}

export default App
