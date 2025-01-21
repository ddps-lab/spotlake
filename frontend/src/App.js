import "./App.css";
import { BrowserRouter, Route } from "react-router-dom";
import Header from "./components/header/header";
import { Home, About, Contact, Document } from "./pages";

function App() {
  return (
    <div className="App">
      <BrowserRouter basename={process.env.PUBLIC_URL}>
        <Header />
        <Route exact path="/" component={Home} />
        <Route exact path="/about" component={About} />
        <Route exact path="/contact" component={Contact} />
        <Route exact path="/document" component={Document} />
      </BrowserRouter>
    </div>
  );
}

export default App;
