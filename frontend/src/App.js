import logo from './logo.svg';
import './App.css';
import {BrowserRouter, Route} from "react-router-dom";
import Header from './components/header/header'
import {Demo, About, Contact, Document} from './pages';

function App() {
  return (
    <div className="App">
      <BrowserRouter basename={process.env.PUBLIC_URL}>
        <Header/>
        <Route exact path="/" component={Demo} />
        <Route exact path="/about" component={About} />
        <Route exact path="/contact" component={Contact} />
        <Route exact path="/document" component={Document} />
      </BrowserRouter>
    </div>
  );
}

export default App;
