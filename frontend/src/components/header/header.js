import React from "react";
import * as style from './styles';
import { useHistory, useLocation } from 'react-router-dom';

function Header () {
    const location = useLocation();
    const history = useHistory();
    const linkTo = ({target}) => {
        const {value} = target;
        history.push(value==="demo"?'/':'/'+value);
    }
    return (
        <style.header>
            <style.title>SpotLake</style.title>
            <style.nav>
                <style.navBtn onClick={linkTo} value='about'>About</style.navBtn>
                <style.navBtn onClick={linkTo} value='document'>Document</style.navBtn>
                <style.navBtn onClick={linkTo} value='demo'>Demo</style.navBtn>
                <style.navBtn onClick={linkTo} value='contact'>Contact</style.navBtn>
            </style.nav>
        </style.header>
    )
}
export default Header;