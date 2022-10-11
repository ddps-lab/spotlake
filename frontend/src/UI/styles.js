import styled from "styled-components";

export const Wrapper = styled.div`
  padding-top: 10px;
  display: flex;
  flex-direction: column;
  align-items: flex-start;
  ${(props) => props.padding ? 'padding : '+ props.padding : ''};
`;
export const Title =styled.h1`
    ${(props) => props.color? 'color : ' : 'color : #000'};
`;
export const SubTitle = styled.h3`
  ${(props) => props.color? 'color : ' : 'color : #222'};
`
export const Box = styled.div`
  display: flex;
  ${(props) => (
      `gap : ${props.gap ? 1 : 0}rem;
      margin : ${props.margin?props.margin : 0};
      ${props.column? 
          props.align ? 'flex-direction: column; align-items : ' + props.align 
                  : 'flex-direction: column;'
          : props.align ? 'flex-direction: row; justify-content : ' + props.align
              :'flex-direction: row;'
      }`
  )};
`;
export const ContentBox = styled.div`
  text-align: left;
  color: #555;
  & ul {
    list-style-type: '- '
  }
  &.about a {
    color : #3d56b2;
  }
`;