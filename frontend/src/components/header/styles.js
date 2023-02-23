import styled from 'styled-components';

export const header = styled.div`
  background: #3D56B2;
  box-shadow: 1px 1px 3px #777;
  position: absolute;
  top: 0;
  width: 100%;
  min-width: 800px;
  padding: 0 10%;
  display: flex;
  flex-direction: row;
  justify-content: space-between;
  align-items: flex-end;
  box-sizing: border-box;
`;
export const title = styled.h2`
  font-size: 2.5em;
  margin: 1em 0 0.2em 0;
  color: #fff;
`;
export const nav = styled.div`
  height: fit-content;
`;
export const navBtn = styled.button`
  border: none;
  color: #fff;
  outline: none;
  background: none;
  font-size: 1.1em;
  box-shadow: none;
  cursor: pointer;
`;
export const forkRibbon = styled.a`
  height: 110%;
  position: absolute;
  left: 0;
  top: 0;
  & img {
    height: 100%;
    width: auto;
  }
`;