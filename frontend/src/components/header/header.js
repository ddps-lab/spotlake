import React from "react";
import * as style from "./styles";
import { useHistory } from "react-router-dom";

function Header() {
  const history = useHistory();
  const linkTo = ({ target }) => {
    const { value } = target;
    history.push(value === "home" || value === undefined ? "/" : "/" + value);
  };
  return (
    <style.header>
      <style.forkRibbon href="https://github.com/ddps-lab/spotlake">
        <img
          decoding="async"
          loading="lazy"
          src="https://github.blog/wp-content/uploads/2008/12/forkme_left_orange_ff7600.png?resize=149%2C149"
          className="attachment-full size-full"
          alt="Fork me on GitHub"
          data-recalc-dims="1"
        />
      </style.forkRibbon>
      <style.title onClick={linkTo} style={{ cursor: "pointer" }}>
        SpotLake
      </style.title>
      <style.nav>
        <style.navBtn onClick={linkTo} value="home">
          Home
        </style.navBtn>
        <style.navBtn onClick={linkTo} value="about">
          About
        </style.navBtn>
        <style.navBtn onClick={linkTo} value="document">
          Document
        </style.navBtn>
        <style.navBtn onClick={linkTo} value="contact">
          Contact
        </style.navBtn>
      </style.nav>
    </style.header>
  );
}
export default Header;
