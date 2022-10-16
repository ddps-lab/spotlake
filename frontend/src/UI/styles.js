import styled from "styled-components";
import {createTheme} from "@mui/material/styles";
import {Button} from "@mui/material";

export const theme = createTheme({
    palette : {
        github : {
            light : "#eee",
            dark : "#b8b9ba",
            main: '#ebf0f4',
            contrastText: '#24292f',
        },
        GoogleForm :{
            light : "rgb(240,235,248)",
            dark : "rgb(130,104,181)",
            main: 'rgba(103,58,183, 0.5)',
            contrastText: '#fff',
        }
    }
})
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
  line-height: 1.4em;
  & ul {
    list-style-type: '- ';
  }
  & a {
    color : #3d56b2;
  }
  & ul.contribute_developers li {
    margin-bottom: 20px;
  }
`;
export const ButtonICon = styled.img`
  max-height: 20px;
  max-width: 20px;
`
export const CustomButton = styled(Button)`
  ${(props) => props.padding? 'padding : '+ props.padding : null} !important;
  margin : ${(props) => props.margin? props.margin : 0} !important;
  text-transform: none !important;
`